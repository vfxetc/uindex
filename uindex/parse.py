from __future__ import print_function

import json
import sys

from .entry import Entry


def iter_entries(fh, pop_path=None, prepend_path=None, search_path=None, invert_search=False, replace_path=None):

    meta = None
    columns = None

    for line_i, line in enumerate(fh):

        line = line.strip()
        if not line:
            continue

        if line.startswith('#'):
            if line.startswith('#scan-start'):
                meta = json.loads(line.split(None, 1)[1])
                columns = meta.get('columns')
            continue

        values = line.split('\t')
        if columns is not None:
            data = dict(zip(columns, values))
        elif len(values) == 7:
            # The first versions didn't specify columns.
            data = dict(zip(
                ('checksum', 'perms', 'size', 'uid', 'gid', 'mtime', 'path'),
                values
            ))
        else:
            print('WARNING: Index parse failure at line {}; {}'.format(line_i, values), file=sys.stderr)
            continue

        entry = Entry(meta=None, **data)

        if pop_path:
            entry.pop_path(pop_path)
        if prepend_path:
            entry.prepend_path(prepend_path)
        if replace_path:
            entry.replace_path(*replace_path)

        if search_path:
            found = entry.search_path(search_path)
            found = (not found) if invert_search else found
            if not found:
                continue
        
        yield entry

