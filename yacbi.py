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
import logging
import fnmatch
import os
import sqlite3

import clang.cindex
import datetime


__all__ = [
    'logger',
    'SourceLocation',
    'Reference',
    'initialize_project',
    'update',
    'get_root_for_path',
    'query_compile_args',
    'query_definitions',
    'query_references',
    'query_subtypes',
    'query_including_files',
    ]


try:
    from logging import NullHandler as _NullLogHandler
except ImportError:
    # python 2.6
    class _NullLogHandler(logging.Handler):
        def emit(self, record):
            pass


logger = logging.getLogger('yacbi')
logger.addHandler(_NullLogHandler())


def _make_absolute_path(cwd, path):
    """Return a normalized, absolute path.

    Arguments:
    cwd -- current working directory
    path -- relative or absolute path
    """
    if os.path.isabs(path):
        return os.path.normpath(path)
    else:
        return os.path.normpath(os.path.join(cwd, path))


_CompileArgs = collections.namedtuple(
    '_CompileArgs', ['all_args', 'includes', 'has_x'])


_CompileCommand = collections.namedtuple(
    '_CompileCommand', ['filename', 'args', 'current_dir', 'is_included'])


SourceLocation = collections.namedtuple(
    'SourceLocation', ['filename', 'line', 'column'])


Reference = collections.namedtuple(
    'Reference', ['location', 'is_definition', 'kind', 'description'])


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


_PATH_ARGS = (
    '-include',
    '-isystem',
    '-I',
    '-iquote',
    '--sysroot=',
    '-isysroot',
)


_CPP_EXTENSIONS = (
    '.cc',
    '.cp',
    '.cxx',
    '.cpp',
    '.CPP',
    '.c++',
    '.C',
)


def _handle_two_part_arg(arg, itr, result_args):
    """Process a compile argument which should be followed by a value.

    Arguments:
    arg -- current argument that needs a value
    itr -- iterator to the list of all arguments being processed
    result_args [out] -- list of filtered arguments
    """
    result_args.append(arg)
    arg = next(itr, None)
    if arg is not None:
        result_args.append(arg)


def _handle_two_part_include_arg(arg, itr, result_args, cwd, includes):
    """Process a compile argument which should be followed by an include path.

    Arguments:
    arg -- current argument that needs a value
    itr -- iterator to the list of all arguments being processed
    result_args [out] -- list of filtered arguments
    cwd -- current working directory
    includes [out] -- list of extra include files provided as arguments
    """
    result_args.append(arg)
    arg = next(itr, None)
    if arg is not None:
        abs_path = _make_absolute_path(cwd, arg)
        if result_args[-1] == '-include':
            includes.add(abs_path)
        result_args.append(abs_path)


def _handle_one_part_include_arg(arg, result_args, cwd, includes):
    """Process a compile argument which contains an include path.

    Arguments:
    arg -- current argument that needs a value
    result_args [out] -- list of filtered arguments
    cwd -- current working directory
    includes [out] -- list of extra include files provided as arguments
    """
    for path_arg in _PATH_ARGS:
        if arg.startswith(path_arg):
            path = arg[len(path_arg):]
            abs_path = _make_absolute_path(cwd, path)
            if path_arg == '-include':
                includes.add(abs_path)
            result_args.append(path_arg + abs_path)
            break


def _make_compile_args(cwd, args, extra_args, banned_args):
    """Create compile arguments.

    Arguments:
    cwd -- current working directory
    args -- list of all arguments as read from the compilation database
    extra_args -- additional arguments that should be appended to args
    banned_args -- arguments that should be ignored
    """
    all_args = []
    includes = set()
    has_x = False
    itr = itertools.chain(args, extra_args)
    arg = next(itr, None)
    while arg is not None:
        if not arg in banned_args:
            if arg in ('-nostdinc',) or arg.startswith(('-D', '-W', '-std=')):
                all_args.append(arg)
            elif arg == '-x':
                has_x = True
                _handle_two_part_arg(arg, itr, all_args)
            elif arg == '-Xpreprocessor':
                _handle_two_part_arg(arg, itr, all_args)
            elif arg in _PATH_ARGS:
                _handle_two_part_include_arg(arg, itr, all_args, cwd, includes)
            else:
                _handle_one_part_include_arg(arg, all_args, cwd, includes)
        arg = next(itr, None)
    return _CompileArgs(all_args, includes, has_x)


def _is_cpp_source(path):
    """Check if a file is a C++ source."""
    ext = os.path.splitext(path)[1]
    return ext in _CPP_EXTENSIONS


class _CompilationDatabase(object):
    """Wrapper around clang.cindex.CompilationDatabase."""

    def __init__(self, root, extra_args, banned_args):
        """Initialize a new instance.

        Arguments:
        root -- parent directory of "compile_commands.json" file
        extra_args -- additional arguments that should be added to commands
        banned_args -- arguments that should be removed from commands
        """
        self._extra_args = extra_args
        self._banned_args = banned_args
        self._path_to_key = {}
        with open(os.path.join(root, 'compile_commands.json')) as cdb:
            for entry in json.load(cdb):
                cwd = entry['directory']
                key = entry['file']
                if not os.path.isabs(key):
                    # keys in Clang's compilation database are absolute
                    # but not normalized
                    key = os.path.join(cwd, key)
                path = os.path.normpath(key)
                self._path_to_key[path] = key
        self._db = clang.cindex.CompilationDatabase.fromDirectory(root)

    def get_all_files(self):
        """Return a set of all files present in this compilation database."""
        return set(self._path_to_key.keys())

    def get_compile_command(self, filename):
        """Return compile command for a given file or None if not found."""
        key = self._path_to_key.get(filename, None)
        if not key:
            return None
        compile_commands = self._db.getCompileCommands(key)
        if not compile_commands:
            return None
        ccmd = compile_commands[0]
        args = _make_compile_args(ccmd.directory,
                                  ccmd.arguments,
                                  self._extra_args,
                                  self._banned_args)
        return _CompileCommand(filename, args, ccmd.directory, False)


def get_root_for_path(path):
    """Return a Yacbi project root for a given path or None if not found.

    For a given path, its Yacbi project root is the closest parent directory
    that contains ".yacbi" directory.
    """
    if os.path.isdir(path):
        current_dir = os.path.dirname(path)
    else:
        current_dir = path
    while True:
        if os.path.isdir(os.path.join(current_dir, '.yacbi')):
            return current_dir
        new_dir = os.path.dirname(current_dir)
        if new_dir == current_dir:
            return None
        current_dir = new_dir


def _init_db(root):
    """Initialize a Yacbi database.

    Arguments:
    root -- Yacbi project root
    """
    dbfile = os.path.join(root, ".yacbi", "index.db")
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
      line INTEGER NOT NULL,
      "column" INTEGER NOT NULL,
      PRIMARY KEY (including_file_id, included_file_id, line, "column"),
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


def _connect_to_db(root):
    """Return a connection to the existing Yacbi database.

    Arguments:
    root -- Yacbi project root
    """
    dbfile = os.path.join(root, ".yacbi", "index.db")
    if not os.path.isfile(dbfile):
        raise RuntimeError("no such file: {0}".format(dbfile))
    conn = sqlite3.connect(
        dbfile,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    return conn


def initialize_project(root):
    yacbi_dir = os.path.join(root, ".yacbi")
    if os.path.exists(yacbi_dir):
        if not os.path.isdir(yacbi_dir):
            raise RuntimeError(
                "{0} exists but is not a directory".format(yacbi_dir))
    else:
        os.makedirs(yacbi_dir)
    _init_db(root)


def query_compile_args(root, filename):
    """Return a list of compile arguments for a given file.

    Arguments:
    root -- root directory of a Yacbi project
    filename -- absolute, normalized file path
    """
    with _connect_to_db(root) as conn:
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
    """Return a list of references (definitions only) for a given USR.

    Arguments:
    root -- root directory of a Yacbi project
    usr -- Clang's Unified Symbol Reference
    """
    with _connect_to_db(root) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM symbols WHERE usr = ? LIMIT 1", (usr,))
        symbol_id = cur.fetchone()
        if not symbol_id:
            return []
        cur.execute("""
            SELECT
                f.path,
                r.line,
                r."column",
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
                r."column" ASC
        """, symbol_id)
        return [Reference(SourceLocation(*t[0:3]),
                          kind=t[3],
                          description=_KIND_TO_DESC.get(t[3], "???"),
                          is_definition=True)
                for t in cur.fetchall()]


def query_references(root, usr):
    """Return a list of all references for a given USR.

    Arguments:
    root -- root directory of a Yacbi project
    usr -- Clang's Unified Symbol Reference
    """
    with _connect_to_db(root) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM symbols WHERE usr = ? LIMIT 1", (usr,))
        symbol_id = cur.fetchone()
        if not symbol_id:
            return []
        cur.execute("""
            SELECT
                f.path,
                r.line,
                r."column",
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
                r."column" ASC
        """, symbol_id)
        return [Reference(SourceLocation(*t[0:3]),
                          kind=t[3],
                          description=_KIND_TO_DESC.get(t[3], "???"),
                          is_definition=t[4])
                for t in cur.fetchall()]


def query_subtypes(root, usr):
    """Return a list of all subtypes for a given USR.

    Arguments:
    root -- root directory of a Yacbi project
    usr -- Clang's Unified Symbol Reference
    """
    with _connect_to_db(root) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM symbols WHERE usr = ? LIMIT 1", (usr,))
        symbol_id = cur.fetchone()
        if not symbol_id:
            return []
        cur.execute("""
            SELECT
                f.path,
                r.line,
                r."column",
                r.kind,
                r.is_definition
            FROM
                refs r LEFT OUTER JOIN
                files f ON (r.file_id = f.id)
            WHERE
                r.symbol_id = ? AND r.kind = 44
            ORDER BY
                f.path ASC,
                r.line ASC,
                r."column" ASC
        """, symbol_id)
        return [Reference(SourceLocation(*t[0:3]),
                          kind=t[3],
                          description=_KIND_TO_DESC.get(t[3], "???"),
                          is_definition=t[4])
                for t in cur.fetchall()]


def query_including_files(root, included_file):
    """Return a list locations where a given file is being included.

    Arguments:
    root -- root directory of a Yacbi project
    included_file -- Clang's Unified Symbol Reference
    """
    with _connect_to_db(root) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM files WHERE path = ? LIMIT 1",
                    (included_file,))
        file_id = cur.fetchone()
        if not file_id:
            return []
        cur.execute("""
            SELECT
                f.path,
                i.line,
                i."column"
            FROM
                includes i LEFT OUTER JOIN
                files f ON (i.including_file_id = f.id)
            WHERE
                i.included_file_id = ?
            ORDER BY
                f.path ASC,
                i.line ASC
        """, file_id)
        return [SourceLocation(*t) for t in cur.fetchall()]


_Config = collections.namedtuple(
    '_Config', ['extra_args', 'banned_args', 'overrides', 'inline_files'])


def _read_config(root):
    config_path = os.path.join(root, ".yacbi", "config.json")
    js = {}
    if os.path.isfile(config_path):
        with open(config_path, 'r') as config_fd:
            js = json.load(config_fd)
    inline_files = set([_make_absolute_path(root, inl)
                        for inl in js.get('inline_files', [])])
    return _Config(js.get('extra_args', []),
                   js.get('banned_args', []),
                   js.get('overrides', []),
                   inline_files)


def update(root):
    config = _read_config(root)
    compilation_db = _CompilationDatabase(
        root,
        config.extra_args,
        config.banned_args)
    with _connect_to_db(root) as conn:
        file_manager = _FileManager(root,
                                    conn,
                                    compilation_db,
                                    config.inline_files)
        for cmd in file_manager:
            logger.info("indexing %s", cmd.filename)
            indexer = Indexer(file_manager, cmd)
            indexer.index()
            if logger.isEnabledFor(logging.ERROR):
                for e in indexer.errors:
                    logger.error("%s:%d:%d: %s",
                                 e.location.filename,
                                 e.location.line,
                                 e.location.column,
                                 e.spelling)
            file_manager.save_indices(indexer.idx_by_path.values())
        file_manager.remove_orphaned_includes()
        conn.commit()


_LocationInFile = collections.namedtuple('_LocationInFile', ['line', 'column'])


_ReferenceData = collections.namedtuple(
    '_ReferenceData', ['is_definition', 'kind'])


_Error = collections.namedtuple(
    '_Error', ['spelling', 'location', 'disable_option'])


_FileInclusion = collections.namedtuple(
    '_FileInclusion', ['included_path', 'line', 'column'])


class _Index(object):
    def __init__(self, cmd):
        self.filename = cmd.filename
        self.cwd = cmd.current_dir
        self.args = cmd.args
        self.includes = set()
        self.references_by_usr = {}
        self.file_id = None
        self.is_included = cmd.is_included
        if not self.args.has_x and _is_cpp_source(self.filename):
            all_args = ['-x', 'c++']
            all_args.extend(self.args.all_args)
            self.child_args = _CompileArgs(all_args, self.cwd, True)
        else:
            self.child_args = self.args

    def add_reference(self, usr, loc, ref):
        refs = self.references_by_usr.get(usr, None)
        if refs is None:
            refs = {loc: ref}
            self.references_by_usr[usr] = refs
        else:
            old_ref = refs.get(loc, None)
            if old_ref is None or ref > old_ref:
                refs[loc] = ref

    def add_include(self, inc):
        self.includes.add(inc)

    def make_child_compile_command(self, child_filename):
        return _CompileCommand(child_filename, self.child_args, self.cwd, True)


class _FileManager(object):
    class File(object):
        def __init__(self, path, last_update, is_included):
            self.path = path
            self.last_update = last_update
            self.is_included = is_included

        def needs_update(self):
            return self.get_mtime() >= self.last_update

        def get_mtime(self):
            return datetime.datetime.fromtimestamp(os.path.getmtime(self.path))

    def __init__(self, root, conn, comp_db, inlines):
        self.root = root + os.path.sep
        self.conn = conn
        self.comp_db = comp_db
        self.inlines = inlines
        self.visited = set()
        self.now = datetime.datetime.now()
        files = self._query_existing_files()
        comp_db_paths = self.comp_db.get_all_files()
        src_paths = set()
        removed_paths = set()
        for f in files:
            if not os.path.exists(f.path):
                removed_paths.add(f.path)
            elif not f.is_included:
                src_paths.add(f.path)
        paths_to_remove = src_paths - comp_db_paths
        self.sources_to_add = comp_db_paths - src_paths
        removed_paths.update(self._remove_files(paths_to_remove))
        self.sources_to_update = set()
        self.headers_to_update = set()
        self.inlines_to_update = set()
        for f in files:
            if f.path not in removed_paths:
                if f.needs_update():
                    if f.is_included:
                        if self._is_inline(f.path):
                            self.inlines_to_update.add(f.path)
                        else:
                            self.headers_to_update.add(f.path)
                    else:
                        self.sources_to_update.add(f.path)
                else:
                    self.visited.add(f.path)

    def _is_inline(self, path):
        for pattern in self.inlines:
            if fnmatch.fnmatchcase(path, pattern):
                return True
        return False

    def should_index(self, path):
        if path in self.visited:
            return False
        self.visited.add(path)
        if path in self.inlines_to_update:
            self.inlines_to_update.remove(path)
            return True
        elif path in self.headers_to_update:
            self.headers_to_update.remove(path)
            return True
        elif path in self.sources_to_add or path in self.sources_to_update:
            return False
        else:
            return path.startswith(self.root)

    def __iter__(self):
        return self

    def save_indices(self, indices):
        for idx in indices:
            file_id = self._save_file(idx.filename, idx.cwd, idx.is_included)
            idx.file_id = file_id
            self._save_args(file_id, idx.args.all_args)
            self._save_refs(file_id, idx.references_by_usr)
        for idx in indices:
            self._save_includes(idx)

    def remove_orphaned_includes(self):
        #
        # DELETE FROM files WHERE is_included = 1 AND
        # NOT EXISTS (SELECT 1 FROM includes i
        # WHERE i.included_file_id = files.id LIMIT 1)
        #
        # seems to be slower in case when there are no orphans.
        cur = self.conn.cursor()
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

    def _save_file(self, path, cwd, is_included):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM files WHERE path = ? LIMIT 1", (path,))
        file_id = cur.fetchone()
        if file_id is None:
            cur.execute("""
                        INSERT INTO files (
                          path,
                          working_dir,
                          last_update,
                          is_included)
                        VALUES (?, ?, ?, ?)""",
                        (path, cwd, self.now, is_included))
            file_id = cur.lastrowid
        else:
            file_id = file_id[0]
            cur.execute("""
                        UPDATE files SET
                          working_dir = ?,
                          last_update = ?,
                          is_included = ?
                        WHERE id = ?""",
                        (cwd, self.now, is_included, file_id))
        return file_id

    def _save_args(self, file_id, args):
        cur = self.conn.cursor()
        cur.execute("""
                    DELETE FROM compile_args
                    WHERE file_id = ?""",
                    (file_id,))
        if args:
            cur.executemany("""
                            INSERT INTO compile_args (
                              file_id,
                              arg)
                            VALUES (?, ?)""",
                            [(file_id, arg) for arg in args])

    def _save_refs(self, file_id, refs_by_usr):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM refs WHERE file_id = ?", (file_id,))
        for usr, refs in refs_by_usr.iteritems():
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
                [(symbol_id, file_id, l.line, l.column, r.kind, r.is_definition)
                 for l, r in refs.iteritems()])

    def _save_includes(self, idx):
        cur = self.conn.cursor()
        cur.execute("""
                    DELETE FROM includes WHERE including_file_id = ?""",
                    (idx.file_id,))
        inc_values = []
        for inc in idx.includes:
            path = inc.included_path
            cur.execute(
                """
                SELECT id FROM files WHERE path = ? LIMIT 1""",
                (path,))
            inc_id = cur.fetchone()
            if inc_id:
                inc_id = inc_id[0]
            elif self.should_index(path):
                # the file must have been empty, so we create a dummy entry
                inc_id = self._save_file(path, self.root, True)
                self._save_args(inc_id, idx.child_args.all_args)
            else:
                # the file is not intended to be stored
                continue
            inc_values.append((idx.file_id, inc_id, inc.line, inc.column))
        if inc_values:
            cur.executemany("""
                            INSERT INTO includes (
                              including_file_id,
                              included_file_id,
                              line,
                              "column")
                            VALUES (?, ?, ?, ?)""",
                            inc_values)

    def next(self):
        if self.sources_to_add:
            path = self.sources_to_add.pop()
            self.visited.add(path)
            return self.comp_db.get_compile_command(path)
        elif self.sources_to_update:
            path = self.sources_to_update.pop()
            self.visited.add(path)
            cmd = self.comp_db.get_compile_command(path)
            if not cmd:
                cmd = self._query_compile_command(path)
            return cmd
        elif self.headers_to_update:
            path = self.headers_to_update.pop()
            self.visited.add(path)
            return self._query_compile_command(path)
        else:
            while self.inlines_to_update:
                inline_path = self.inlines_to_update.pop()
                path = self._query_including_file(inline_path)
                if path:
                    self.visited.add(path)
                    return self._query_compile_command(path)
            raise StopIteration

    def _query_existing_files(self):
        cur = self.conn.cursor()
        cur.execute("""
                    SELECT
                      path,
                      last_update as "last_update [timestamp]",
                      is_included
                    FROM files
                    ORDER BY path""")
        return [self.File(*tup) for tup in cur.fetchall()]

    def _query_including_file(self, path):
        cur = self.conn.cursor()
        cur.execute("""
                    SELECT
                      id
                    FROM files
                    WHERE path = ?""",
                    (path,))
        included_file_id = cur.fetchone()
        if not included_file_id:
            return None
        cur.execute("""
                    SELECT
                      f.path
                    FROM includes i
                    LEFT OUTER JOIN files f ON (i.including_file_id = f.id)
                    WHERE
                      i.included_file_id = ?
                    ORDER BY
                      f.last_update DESC,
                      f.id ASC
                    LIMIT 1""",
                    included_file_id)
        including_file_path = cur.fetchone()
        if not including_file_path:
            return None
        return including_file_path[0]

    def _query_compile_command(self, path):
        cur = self.conn.cursor()
        cur.execute("""
                    SELECT
                      id,
                      working_dir,
                      is_included
                    FROM files
                    WHERE path = ?""",
                    (path,))
        file_id, cwd, is_included = cur.fetchone()
        cur.execute("""
                    SELECT arg
                    FROM compile_args
                    WHERE file_id = ?
                    ORDER BY id""",
                    (file_id,))
        args = [tup[0] for tup in cur.fetchall()]
        return _CompileCommand(path,
                               _make_compile_args(cwd, args, [], []),
                               cwd,
                               is_included)

    def _remove_files(self, paths):
        cur = self.conn.cursor()
        removed = set()
        for path in paths:
            cur.execute("SELECT id FROM files WHERE path = ? LIMIT 1",
                        (path,))
            file_id = cur.fetchone()
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
                removed.add(path)
        return removed


class Indexer(object):
    def __init__(self, file_manager, cmd):
        self.file_manager = file_manager
        self.filename = cmd.filename
        self.cwd = cmd.current_dir
        self.args = cmd.args
        self.src_index = _Index(cmd)
        self.is_included = cmd.is_included
        self.idx_by_path = {self.filename: self.src_index}
        self.errors = []

    def index(self):
        clang_index = clang.cindex.Index.create()
        logger.debug("parsing %s: %s",
                     self.filename,
                     " ".join(self.args.all_args))
        unit = clang_index.parse(
            self.filename,
            self.args.all_args,
            None,
            clang.cindex.TranslationUnit.PARSE_INCOMPLETE |
            clang.cindex.TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD)
        self._find_references(unit.cursor)
        self._sort_includes(unit.get_includes())
        self._populate_errors(unit.diagnostics)

    def _find_references(self, cursor):
        location = cursor.location
        if not location.file:
            for child_cursor in cursor.get_children():
                self._find_references(child_cursor)
        else:
            path = unicode(os.path.abspath(location.file.name))
            idx = self._get_index(path)
            if idx:
                if cursor.referenced:
                    usr = cursor.referenced.get_usr()
                    if usr and usr != "c:":
                        idx.add_reference(
                            usr,
                            _LocationInFile(location.line, location.column),
                            _ReferenceData(cursor.is_definition(),
                                           cursor.kind.from_param()))
                for child_cursor in cursor.get_children():
                    self._find_references(child_cursor)

    def _get_index(self, path):
        idx = self.idx_by_path.get(path, None)
        if not idx and self.file_manager.should_index(path):
            idx = self._make_child_index(path)
            self.idx_by_path[path] = idx
        return idx

    def _make_child_index(self, path):
        return _Index(self.src_index.make_child_compile_command(path))

    def _populate_errors(self, diags):
        def translate_diag(diag):
            loc = diag.location
            if loc.file:
                filename = loc.file.name
            else:
                filename = self.filename
            return _Error(diag.spelling,
                          SourceLocation(filename, loc.line, loc.column),
                          diag.disable_option)
        self.errors = [translate_diag(d)
                       for d in diags
                       if d.severity >= clang.cindex.Diagnostic.Error]

    def _sort_includes(self, includes):
        if not self.is_included:
            for inc in self.args.includes:
                self.src_index.add_include(_FileInclusion(inc, 0, 0))
        for inc in includes:
            if inc.source:
                idx = self.idx_by_path.get(inc.source.name, None)
                if idx:
                    idx.add_include(_FileInclusion(
                        os.path.normpath(inc.include.name),
                        inc.location.line,
                        inc.location.column))
