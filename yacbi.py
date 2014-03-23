"""
yacbi - Yet Another Clang-Based Indexer.

Copyright (C) 2014 Jakub Lewandowski <jakub.lewandowski@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import clang.cindex
import collections
import datetime
import itertools
import json
import os
import shutil
import sqlite3
import tempfile


__all__ = [
    'CompilationDatabase',
    'CompileArgs',
    'CompileCommand',
    'Indexer',
    'connect_to_db',
    ]


def _make_absolute_path(cwd, path):
    if os.path.isabs(path):
        return os.path.normpath(path)
    else:
        return os.path.normpath(os.path.join(cwd, path))


CompileArgs = collections.namedtuple(
    'CompileArgs', ['all_args', 'iincludes', 'has_x'])


CompileCommand = collections.namedtuple(
    'CompileCommand', ['filename', 'args', 'current_dir'])


def _make_compile_args(cwd, args, extra_args, banned_args):
    path_args = ('-include',
                 '-isystem',
                 '-I',
                 '-iquote',
                 '--sysroot=',
                 '-isysroot',)
    all_args = []
    iincludes = set()
    has_x = False
    done = object()
    itr = itertools.chain(args, extra_args)
    arg = next(itr, done)
    while arg is not done:
        if not arg in banned_args:
            if arg in ('-nostdinc',):
                all_args.append(arg)
            elif arg in ('-x', '-Xpreprocessor',):
                if arg == '-x':
                    has_x = True
                all_args.append(arg)
                arg = next(itr, done)
                if arg is not done:
                    all_args.append(arg)
            elif arg.startswith(('-D', '-W', '-std=',)):
                all_args.append(arg)
            elif arg in path_args:
                all_args.append(arg)
                arg = next(itr, done)
                if arg is not done:
                    abs_path = _make_absolute_path(cwd, arg)
                    if all_args[-1] == '-include':
                        iincludes.add(abs_path)
                    all_args.append(abs_path)
            else:
                for path_arg in path_args:
                    if arg.startswith(path_arg):
                        path = arg[len(path_arg):]
                        abs_path = _make_absolute_path(cwd, path)
                        if path_arg == '-include':
                            iincludes.add(abs_path)
                        all_args.append(path_arg + abs_path)
                        break
        arg = next(itr, done)
    return CompileArgs(all_args, iincludes, has_x)


def _is_cpp_source(path):
    base, ext = os.path.splitext(path)
    cpp_extensions = (
        '.cc',
        '.cp',
        '.cxx',
        '.cpp',
        '.CPP',
        '.c++',
        '.C',)
    return ext in cpp_extensions


class CompilationDatabase(object):
    def __init__(self, comp_db_dir, extra_args, banned_args):
        self._extra_args = extra_args
        self._banned_args = banned_args
        self._all_files = set()
        db_json = None
        db_filename = 'compile_commands.json'
        comp_db_path = os.path.join(comp_db_dir, db_filename)
        with open(comp_db_path, 'r') as cdb:
            db_json = json.load(cdb)
        for entry in db_json:
            self._all_files.add(entry['file'])
        tmp_dir = tempfile.mkdtemp()
        tmp_db_path = os.path.join(tmp_dir, db_filename)
        with open(tmp_db_path, 'w') as cdb:
            json.dump(db_json, cdb)
        self._db = clang.cindex.CompilationDatabase.fromDirectory(tmp_dir)
        shutil.rmtree(tmp_dir)

    def get_all_files(self):
        return self._all_files

    def get_compile_command(self, filename):
        compile_commands = self._db.getCompileCommands(filename)
        if not compile_commands:
            raise KeyError(filename)
        cc = compile_commands[0]
        args = _make_compile_args(cc.directory,
                                  cc.arguments,
                                  self._extra_args,
                                  self._banned_args)
        return CompileCommand(filename, args, cc.directory)


def connect_to_db(dbfile):
    conn = sqlite3.connect(
        dbfile,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA foreign_keys=ON;

    CREATE TABLE IF NOT EXISTS files (
      id INTEGER NOT NULL,
      path VARCHAR,
      working_dir VARCHAR,
      last_update DATETIME,
      origin INTEGER,
      PRIMARY KEY (id),
      UNIQUE (path)
    );

    CREATE TABLE IF NOT EXISTS compile_args (
      id INTEGER NOT NULL,
      file_id INTEGER NOT NULL,
      arg VARCHAR,
      PRIMARY KEY (id),
      FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS includes (
      including_file_id INTEGER NOT NULL,
      included_file_id INTEGER NOT NULL,
      PRIMARY KEY (including_file_id, included_file_id),
      FOREIGN KEY (including_file_id) REFERENCES files (id) ON DELETE CASCADE,
      FOREIGN KEY (included_file_id) REFERENCES files (id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS symbols (
      id INTEGER NOT NULL,
      usr VARCHAR,
      PRIMARY KEY (id),
      UNIQUE (usr)
    );

    CREATE TABLE IF NOT EXISTS refs (
      symbol_id INTEGER NOT NULL,
      file_id INTEGER NOT NULL,
      line INTEGER NOT NULL,
      "column" INTEGER NOT NULL,
      kind INTEGER NOT NULL,
      PRIMARY KEY (symbol_id, file_id, line, "column", kind),
      FOREIGN KEY (symbol_id) REFERENCES symbols (id) ON DELETE CASCADE,
      FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE
    );
    """)
    conn.commit()
    return conn


class Indexer(object):
    _Reference = collections.namedtuple('_Reference',
                                        ['line', 'column', 'kind'])

    class _IndexResult(object):
        def __init__(self, filename, cwd, args):
            self.filename = filename
            self.cwd = cwd
            self.args = args
            self.diagnostics = []
            self.includes = []
            self.references_by_usr = {}
            self.file_id = None
            self.is_included = None

        def add_reference(self, usr, ref):
            refs = self.references_by_usr.get(usr, None)
            if refs is None:
                refs = set((ref,))
                self.references_by_usr[usr] = refs
            else:
                refs.add(ref)

    def __init__(self, conn, comp_db, project_root):
        self._conn = conn
        self._comp_db = comp_db
        self._root = project_root
        self._now = datetime.datetime.now()

    def run(self):
        self._now = datetime.datetime.now()
        add_files, rm_files = self._get_adds_and_removes()
        self._remove_files_from_db(rm_files)
        self._process_files(
            [self._comp_db.get_compile_command(src) for src in add_files],
            False)
        self._process_files(
            self._get_commands_for_updates(),
            True)
        self._remove_orphaned_includes()

    def _get_adds_and_removes(self):
        cur = self._conn.cursor()
        cur.execute('SELECT path FROM files WHERE origin = 0')
        db_files = set()
        for tup in cur.fetchall():
            db_files.add(tup[0])
        comp_db_files = self._comp_db.get_all_files()
        rm_files = db_files - comp_db_files
        add_files = comp_db_files - db_files
        return add_files, rm_files

    def _remove_files_from_db(self, filenames):
        cur = self._conn.cursor()
        for filename in filenames:
            cur.execute("DELETE FROM files WHERE path = ?", (filename,))

    def _remove_orphaned_includes(self):
        cur = self._conn.cursor()
        while True:
            cur.execute("SELECT id FROM files WHERE origin = 1")
            all_includes = set(cur.fetchall())
            cur.execute("SELECT DISTINCT included_file_id FROM includes")
            included = set(cur.fetchall())
            orphans = all_includes - included
            if not orphans:
                break
            for inc in orphans:
                cur.execute("DELETE FROM files WHERE id = ?", inc)

    def _process_files(self, compile_commands, is_update):
        cur = self._conn.cursor()
        indexed_so_far = set()
        indexed_files = []
        commands = compile_commands
        is_included = False
        while commands:
            new_commands = []
            for cmd in commands:
                idx = self._index_file(cmd)
                idx.is_included = is_included
                indexed_files.append(idx)
                indexed_so_far.add(idx.filename)
                print idx.filename, len(idx.diagnostics)
                for inc in idx.includes:
                    if inc in indexed_so_far:
                        continue
                    indexed_so_far.add(inc)
                    cur.execute("SELECT id FROM files WHERE path = ?", (inc,))
                    if cur.fetchone() is None:
                        if (not is_included and _is_cpp_source(idx.filename)
                           and not idx.args.has_x):
                            all_args = ['-x', 'c++']
                            all_args.extend(idx.args.all_args)
                            args = CompileArgs(all_args,
                                               idx.args.iincludes,
                                               True)
                        else:
                            args = idx.args
                        new_commands.append(CompileCommand(inc, args, idx.cwd))
            commands = new_commands
            is_included = True
        for idx in indexed_files:
            self._save_file_index(idx)
        for idx in indexed_files:
            self._save_includes(idx)

    def _get_commands_for_updates(self):
        cur = self._conn.cursor()
        cur.execute("""
                    SELECT id,
                      path,
                      last_update as "last_update [timestamp]",
                      working_dir
                    FROM files""")
        update_commands = []

        def get_mtime(path):
            return datetime.datetime.fromtimestamp(os.path.getmtime(path))

        for tup in cur.fetchall():
            if os.path.isfile(tup[1]):
                if get_mtime(tup[1]) >= tup[2]:
                    cur.execute("""
                                SELECT arg FROM compile_args WHERE file_id = ?
                                ORDER BY id""",
                                (tup[0],))
                    args = [arg[0] for arg in cur.fetchall()]
                    cwd = tup[3]
                    cmd = CompileCommand(tup[1],
                                         _make_compile_args(cwd, args, [], []),
                                         cwd)
                    update_commands.append(cmd)
        return update_commands

    def _save_file_index(self, idx):
        cur = self._conn.cursor()
        cur.execute("""
                    SELECT id FROM files WHERE path = ? LIMIT 1 OFFSET 0""",
                    (idx.filename,))
        file_id = cur.fetchone()
        if file_id is None:
            if idx.is_included:
                origin = 1
            else:
                origin = 0
            cur.execute("""
                        INSERT INTO files (
                          path,
                          working_dir,
                          last_update,
                          origin)
                        VALUES (?, ?, ?, ?)""",
                        (idx.filename, idx.cwd, self._now, origin))
            file_id = cur.lastrowid
        else:
            file_id = file_id[0]
            cur.execute("""
                        UPDATE files SET
                          working_dir = ?,
                          last_update = ?
                        WHERE id = ?""",
                        (idx.cwd, self._now, file_id))
        idx.file_id = file_id
        cur.execute("""
                    DELETE FROM compile_args
                    WHERE file_id = ?""",
                    (idx.file_id,))
        if idx.args:
            cur.executemany("""
                            INSERT INTO compile_args (
                              file_id,
                              arg)
                            VALUES (?, ?)""",
                            [(idx.file_id, arg) for arg in idx.args.all_args])
        cur.execute("DELETE FROM refs WHERE file_id = ?", (idx.file_id,))
        for usr, refs in idx.references_by_usr.iteritems():
            cur.execute("""
                        SELECT id FROM symbols
                        WHERE usr = ? LIMIT 1 OFFSET 0""",
                        (usr,))
            symbol_id = cur.fetchone()
            if symbol_id is None:
                cur.execute("INSERT INTO symbols (usr) VALUES (?)", (usr,))
                symbol_id = cur.lastrowid
            else:
                symbol_id = symbol_id[0]
                cur.executemany(
                    """
                    INSERT INTO refs (
                      symbol_id,
                      file_id,
                      line,
                      "column",
                      kind)
                    VALUES (?, ?, ?, ?, ?)""",
                    [(symbol_id, idx.file_id, ref.line, ref.column, ref.kind)
                     for ref in refs])

    def _save_includes(self, idx):
        cur = self._conn.cursor()
        cur.execute("""
                    DELETE FROM includes WHERE including_file_id = ?""",
                    (idx.file_id,))
        inc_values = []
        for inc in idx.includes:
            cur.execute(
                """
                SELECT id FROM files WHERE path = ? LIMIT 1 OFFSET 0""",
                (inc,))
            inc_id = cur.fetchone()
            inc_values.append((idx.file_id, inc_id[0]))
        if inc_values:
            cur.executemany("""
                            INSERT INTO includes (
                              including_file_id,
                              included_file_id)
                            VALUES (?, ?)""",
                            inc_values)

    def _filter_includes(self, all_includes, cwd):
        direct_includes = set()
        for inc in all_includes:
            if inc.depth < 2:
                path = _make_absolute_path(cwd, inc.include.name)
                if path.startswith(self._root + os.path.sep):
                    direct_includes.add(path)
        return direct_includes

    def _index_file(self, cmd):
        clang_idx = clang.cindex.Index.create()
        tu = clang_idx.parse(
            cmd.filename,
            cmd.args.all_args,
            None,
            clang.cindex.TranslationUnit.PARSE_INCOMPLETE)
        idx = Indexer._IndexResult(cmd.filename, cmd.current_dir, cmd.args)

        def find_references(cursor, idx):
            if not cursor.location.file:
                for child_cursor in cursor.get_children():
                    find_references(child_cursor, idx)
            elif cursor.location.file.name == idx.filename:
                if cursor.referenced:
                    usr = cursor.referenced.get_usr()
                    if usr and usr != "c:":
                        idx.add_reference(
                            usr,
                            Indexer._Reference(cursor.location.line,
                                               cursor.location.column,
                                               cursor.kind.from_param()))
                for child_cursor in cursor.get_children():
                    find_references(child_cursor, idx)

        find_references(tu.cursor, idx)
        idx.diagnostics = [repr(d) for d in tu.diagnostics]
        print "indexing", cmd.filename
        idx.includes = self._filter_includes(
            tu.get_includes(),
            cmd.current_dir)
        for inc in cmd.args.iincludes:
            idx.includes.add(inc)
        return idx

