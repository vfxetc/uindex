from __future__ import print_function

import collections
import json
import sys

_Token = collections.namedtuple('_Token', ['path', 'checksum', 'perms', 'size', 'uid', 'gid', 'mtime', 'inode', 'raw_time'])
class Token(_Token):

    def __new__(cls, path, checksum, perms, size, uid, gid, mtime, inode=None, prepend_path=None):
        if prepend_path:
            path = prepend_path + path
        return super(Token, cls).__new__(cls,
            path,
            checksum,
            int(perms, 8),
            int(size),
            int(uid),
            int(gid),
            float(mtime),
            int(inode) if inode else None,
            mtime,
        )

    @property
    def epsilon(self):
        try:
            digits = self._raw_time.split('.')[1]
        except IndexError:
            return 0
        return 2 * 10 ** -digits



def iter_raw_index(fh, prepend_path=None):

    prepend_path = prepend_path.strip('/') + '/' if prepend_path else prepend_path

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
            print('WARNING: Index parse failure at line {}: {!r}'.format(line_i, line), file=sys.stderr)
            continue

        if prepend_path:
            data['prepend_path'] = prepend_path

        yield Token(**data)


def prompt_bool(prompt, default=True):
    while True:
        res = raw_input(prompt + ' [{}{}]: '.format('yY'[default], 'Nn'[default])).strip()
        if not res:
            return default
        if res in ('y', 'Y', 'yes'):
            return True
        if res in ('n', 'N', 'no'):
            return False
