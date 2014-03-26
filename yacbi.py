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
import collections
import itertools
import json
import os
import sqlite3

import clang.cindex
import datetime


__all__ = [
    'CompilationDatabase',
    'CompileArgs',
    'CompileCommand',
    'Reference',
    'Indexer',
    'connect_to_db',
    'get_root_for_path',
    'query_compile_args',
    'query_definitions',
    'query_references',
    ]


def _make_absolute_path(cwd, path):
    if os.path.isabs(path):
        return os.path.normpath(path)
    else:
        return os.path.normpath(os.path.join(cwd, path))


CompileArgs = collections.namedtuple(
    'CompileArgs', ['all_args', 'iincludes', 'has_x'])


CompileCommand = collections.namedtuple(
    'CompileCommand', ['filename', 'args', 'current_dir', 'is_included'])


Reference = collections.namedtuple(
    'Reference',
    ['filename', 'line', 'column', 'is_definition', 'kind', 'description'])


_KIND_TO_DESC = {
    1: 'type declaration',
    2: 'struct declaration',
    3: 'union declaration',
    4: 'class declaration',
    5: 'enum declaration',
    6: 'member declaration',
    7: 'enum constant declaration',
    8: 'function declaration',
    9: 'variable declaration',
    10: 'argument declaration',
    20: 'typedef declaration',
    21: 'method declaration',
    22: 'namespace declaration',
    24: 'constructor declaration',
    25: 'destructor declaration',
    26: 'conversion function declaration',
    27: 'template type parameter',
    28: 'non-type template parameter',
    29: 'template template parameter',
    30: 'function template declaration',
    31: 'class template declaration',
    32: 'class template partial specialization',
    33: 'namespace alias',
    43: 'type reference',
    44: 'base specifier',
    45: 'template reference',
    46: 'namespace reference',
    47: 'member reference',
    48: 'label reference',
    49: 'overloaded declaration reference',
    100: 'expression',
    101: 'reference',
    102: 'member reference',
    103: 'function call',
    501: 'macro definition',
    502: 'macro instantiation',
}


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
    ext = os.path.splitext(path)[1]
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
    def __init__(self, root, extra_args, banned_args):
        self._extra_args = extra_args
        self._banned_args = banned_args
        self._all_files = set()
        db_path = os.path.join(root, 'compile_commands.json')
        with open(db_path) as cdb:
            for entry in json.load(cdb):
                self._all_files.add(entry['file'])
        self._db = clang.cindex.CompilationDatabase.fromDirectory(root)

    def get_all_files(self):
        return self._all_files

    def get_compile_command(self, filename):
        compile_commands = self._db.getCompileCommands(filename)
        if not compile_commands:
            raise KeyError(filename)
        ccmd = compile_commands[0]
        args = _make_compile_args(ccmd.directory,
                                  ccmd.arguments,
                                  self._extra_args,
                                  self._banned_args)
        return CompileCommand(filename, args, ccmd.directory, False)


def get_root_for_path(path):
    if os.path.isdir(path):
        current_dir = os.path.dirname(path)
    else:
        current_dir = path
    while True:
        if os.path.isfile(os.path.join(current_dir, '.yacbi.db')):
            return current_dir
        new_dir = os.path.dirname(current_dir)
        if new_dir == current_dir:
            return None
        current_dir = new_dir


def connect_to_db(root_dir):
    dbfile = os.path.join(root_dir, '.yacbi.db')
    conn = sqlite3.connect(
        dbfile,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA foreign_keys=ON;

    CREATE TABLE IF NOT EXISTS files (
      id INTEGER NOT NULL,
      path VARCHAR NOT NULL,
      working_dir VARCHAR NOT NULL,
      last_update DATETIME NOT NULL,
      is_included BOOL NOT NULL,
      PRIMARY KEY (id),
      UNIQUE (path)
    );

    CREATE TABLE IF NOT EXISTS compile_args (
      id INTEGER NOT NULL,
      file_id INTEGER NOT NULL,
      arg VARCHAR NOT NULL,
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
      usr VARCHAR NOT NULL,
      PRIMARY KEY (id),
      UNIQUE (usr)
    );

    CREATE TABLE IF NOT EXISTS refs (
      symbol_id INTEGER NOT NULL,
      file_id INTEGER NOT NULL,
      line INTEGER NOT NULL,
      "column" INTEGER NOT NULL,
      kind INTEGER NOT NULL,
      is_definition BOOL NOT NULL,
      PRIMARY KEY (symbol_id, file_id, line, "column"),
      FOREIGN KEY (symbol_id) REFERENCES symbols (id) ON DELETE CASCADE,
      FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE
    );
    """)
    conn.commit()
    return conn


def query_compile_args(root, filename):
    with connect_to_db(root) as conn:
        cur = conn.cursor()
        cur.execute("""SELECT id FROM files WHERE path = ?""", (filename,))
        file_id = cur.fetchone()
        if file_id is None:
            return None
        cur.execute("""
                    SELECT arg FROM compile_args
                    WHERE file_id = ?
                    ORDER BY id""",
                    file_id)
        return [tup[0] for tup in cur.fetchall()]


def query_definitions(root, usr):
    with connect_to_db(root) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM symbols WHERE usr = ? LIMIT 1", (usr,))
        symbol_id = cur.fetchone()
        if not symbol_id:
            return []
        cur.execute("""
            SELECT
                f.path,
                r.line,
                r.column,
                r.kind
            FROM
                refs r LEFT OUTER JOIN
                files f ON (r.file_id = f.id)
            WHERE
                r.is_definition = 1 AND
                r.symbol_id = ?
            ORDER BY
                f.path ASC,
                r.line ASC,
                r.column ASC
        """, symbol_id)
        return [Reference(filename=t[0],
                          line=t[1],
                          column=t[2],
                          kind=t[3],
                          description=_KIND_TO_DESC.get(t[3], "???"),
                          is_definition=True)
                for t in cur.fetchall()]


def query_references(root, usr):
    with connect_to_db(root) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM symbols WHERE usr = ? LIMIT 1", (usr,))
        symbol_id = cur.fetchone()
        if not symbol_id:
            return []
        cur.execute("""
            SELECT
                f.path,
                r.line,
                r.column,
                r.kind,
                r.is_definition
            FROM
                refs r LEFT OUTER JOIN
                files f ON (r.file_id = f.id)
            WHERE
                r.symbol_id = ?
            ORDER BY
                r.is_definition DESC,
                f.path ASC,
                r.line ASC,
                r.column ASC
        """, symbol_id)
        return [Reference(filename=t[0],
                          line=t[1],
                          column=t[2],
                          kind=t[3],
                          description=_KIND_TO_DESC.get(t[3], "???"),
                          is_definition=t[4])
                for t in cur.fetchall()]


class Indexer(object):
    _RefLocation = collections.namedtuple('_RefLocation', ['line', 'column'])

    _Ref = collections.namedtuple('_Ref', ['is_definition', 'kind'])

    class _IndexResult(object):
        def __init__(self, cmd):
            self.filename = cmd.filename
            self.cwd = cmd.current_dir
            self.args = cmd.args
            self.diagnostics = []
            self.includes = []
            self.references_by_usr = {}
            self.file_id = None
            self.is_included = cmd.is_included

        def add_reference(self, usr, loc, ref):
            refs = self.references_by_usr.get(usr, None)
            if refs is None:
                refs = {loc: ref}
                self.references_by_usr[usr] = refs
            else:
                old_ref = refs.get(loc, None)
                if old_ref is None or ref > old_ref:
                    refs[loc] = ref

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
            [self._comp_db.get_compile_command(src) for src in add_files])
        self._process_files(self._get_commands_for_updates())
        self._remove_orphaned_includes()

    def _get_adds_and_removes(self):
        cur = self._conn.cursor()
        cur.execute('SELECT path FROM files WHERE is_included = 0')
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
            cur.execute("SELECT id FROM files WHERE path = ? LIMIT 1",
                        (filename,))
            file_id = cur.fetchone()
            if file_id:
                cur.execute("""
                            SELECT EXISTS (
                              SELECT 1
                              FROM includes
                              WHERE included_file_id = ?
                              LIMIT 1)""",
                            file_id)
                if cur.fetchone()[0]:
                    cur.execute(
                        "UPDATE files SET is_included = 1 WHERE id = ?",
                        file_id)
                else:
                    cur.execute("DELETE FROM files WHERE id = ?", file_id)

    def _remove_orphaned_includes(self):
        #
        # DELETE FROM files WHERE is_included = 1 AND
        # NOT EXISTS (SELECT 1 FROM includes i
        # WHERE i.included_file_id = files.id LIMIT 1)
        #
        # seems to be slower in case when there are no orphans.
        cur = self._conn.cursor()
        while True:
            cur.execute("SELECT id FROM files WHERE is_included = 1")
            all_includes = set(cur.fetchall())
            cur.execute("SELECT DISTINCT included_file_id FROM includes")
            included = set(cur.fetchall())
            orphans = all_includes - included
            if not orphans:
                break
            for inc in orphans:
                cur.execute("DELETE FROM files WHERE id = ?", inc)

    def _process_files(self, compile_commands):
        cur = self._conn.cursor()
        indexed_so_far = set()
        indexed_files = []
        commands = compile_commands
        while commands:
            new_commands = []
            for cmd in commands:
                idx = self._index_file(cmd)
                indexed_files.append(idx)
                indexed_so_far.add(idx.filename)
                print idx.filename, len(idx.diagnostics)
                for inc in idx.includes:
                    if inc in indexed_so_far:
                        continue
                    indexed_so_far.add(inc)
                    cur.execute("SELECT id FROM files WHERE path = ?", (inc,))
                    if cur.fetchone() is None:
                        if (not idx.is_included
                                and not idx.args.has_x
                                and _is_cpp_source(idx.filename)):
                            all_args = ['-x', 'c++']
                            all_args.extend(idx.args.all_args)
                            args = CompileArgs(all_args,
                                               idx.args.iincludes,
                                               True)
                        else:
                            args = idx.args
                        new_cmd = CompileCommand(inc, args, idx.cwd, True)
                        new_commands.append(new_cmd)
            commands = new_commands
        for idx in indexed_files:
            self._save_file_index(idx)
        for idx in indexed_files:
            self._save_includes(idx)

    @staticmethod
    def _get_mtime(path):
        return datetime.datetime.fromtimestamp(os.path.getmtime(path))

    def _get_commands_for_updates(self):
        cur = self._conn.cursor()
        cur.execute("""
                    SELECT id,
                      path,
                      last_update as "last_update [timestamp]",
                      working_dir,
                      is_included
                    FROM files""")
        update_commands = []
        for tup in cur.fetchall():
            if os.path.isfile(tup[1]):
                if self._get_mtime(tup[1]) >= tup[2]:
                    cur.execute("""
                                SELECT arg FROM compile_args WHERE file_id = ?
                                ORDER BY id""",
                                (tup[0],))
                    args = [arg[0] for arg in cur.fetchall()]
                    cwd = tup[3]
                    cmd = CompileCommand(tup[1],
                                         _make_compile_args(cwd, args, [], []),
                                         cwd,
                                         tup[4])
                    update_commands.append(cmd)
        return update_commands

    def _save_file_index(self, idx):
        cur = self._conn.cursor()
        cur.execute("SELECT id FROM files WHERE path = ? LIMIT 1",
                    (idx.filename,))
        file_id = cur.fetchone()
        if file_id is None:
            cur.execute("""
                        INSERT INTO files (
                          path,
                          working_dir,
                          last_update,
                          is_included)
                        VALUES (?, ?, ?, ?)""",
                        (idx.filename, idx.cwd, self._now, idx.is_included))
            file_id = cur.lastrowid
        else:
            file_id = file_id[0]
            cur.execute("""
                        UPDATE files SET
                          working_dir = ?,
                          last_update = ?,
                          is_included = ?
                        WHERE id = ?""",
                        (idx.cwd, self._now, idx.is_included, file_id))
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
                        WHERE usr = ? LIMIT 1""",
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
                  kind,
                  is_definition)
                VALUES (?, ?, ?, ?, ?, ?)""",
                self._make_ref_values(symbol_id, idx.file_id, refs))

    @staticmethod
    def _make_ref_values(symbol_id, file_id, refs):
        return [(symbol_id, file_id, l.line, l.column, r.kind, r.is_definition)
                for l, r in refs.iteritems()]

    def _save_includes(self, idx):
        cur = self._conn.cursor()
        cur.execute("""
                    DELETE FROM includes WHERE including_file_id = ?""",
                    (idx.file_id,))
        inc_values = []
        for inc in idx.includes:
            cur.execute(
                """
                SELECT id FROM files WHERE path = ? LIMIT 1""",
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

    @staticmethod
    def _find_references(cursor, idx):
        if not cursor.location.file:
            for child_cursor in cursor.get_children():
                Indexer._find_references(child_cursor, idx)
        elif cursor.location.file.name == idx.filename:
            if cursor.referenced:
                usr = cursor.referenced.get_usr()
                if usr and usr != "c:":
                    idx.add_reference(
                        usr,
                        Indexer._RefLocation(cursor.location.line,
                                             cursor.location.column),
                        Indexer._Ref(cursor.is_definition(),
                                     cursor.kind.from_param()))
            for child_cursor in cursor.get_children():
                Indexer._find_references(child_cursor, idx)

    def _index_file(self, cmd):
        clang_idx = clang.cindex.Index.create()
        unit = clang_idx.parse(
            cmd.filename,
            cmd.args.all_args,
            None,
            clang.cindex.TranslationUnit.PARSE_INCOMPLETE |
            clang.cindex.TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD)
        idx = Indexer._IndexResult(cmd)
        self._find_references(unit.cursor, idx)
        idx.diagnostics = [repr(d) for d in unit.diagnostics]
        print "indexing", cmd.filename
        idx.includes = self._filter_includes(
            unit.get_includes(),
            cmd.current_dir)
        for inc in cmd.args.iincludes:
            idx.includes.add(inc)
        return idx
