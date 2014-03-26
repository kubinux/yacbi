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
import logging
import os
import sys

import yacbi


def main():
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    yacbi.logger.addHandler(stderr_handler)
    yacbi.logger.setLevel(logging.ERROR)
    yacbi.create_or_update(os.getcwd())


if __name__ == '__main__':
    main()
