#!python

"""
yacbi-index

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
import json
import os
import yacbi


Config = collections.namedtuple(
    'Config', ['extra_args', 'banned_args', 'overrides'])


def read_config():
    config_filename = '.yacbi.json'
    js = {}
    if os.path.isfile(config_filename):
        with open(config_filename, 'r') as config_fd:
            js = json.load(config_fd)
    return Config(js.get('extra_args', []),
                  js.get('banned_args', []),
                  js.get('overrides', []))


def main():
    config = read_config()
    root = os.getcwd()
    compilation_db = yacbi.CompilationDatabase(
        root,
        config.extra_args,
        config.banned_args)
    with yacbi.connect_to_db('index.db') as conn:
        indexer = yacbi.Indexer(conn, compilation_db, root)
        indexer.run()
        conn.commit()


if __name__ == '__main__':
    main()
