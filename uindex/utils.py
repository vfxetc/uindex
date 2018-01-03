import collections


Token = collections.namedtuple('Token', ['path', 'checksum', 'perms', 'size', 'uid', 'gid', 'mtime'])

def iter_raw_index(fh):
    for line_i, line in enumerate(fh):
        line = line.strip()
        if not line:
            continue
        try:
            checksum, perms, size, uid, gid, mtime, path = line.split('\t')
        except ValueError as e:
            print 'WARNING: Index parse failure at line {}: {}'.format(line_i, e)
            return
        yield Token(path, checksum, int(perms), int(size), int(uid), int(gid), float(mtime))


def prompt_bool(prompt, default=True):
    while True:
        res = raw_input(prompt + ' [{}{}]: '.format('yY'[default], 'Nn'[default])).strip()
        if not res:
            return default
        if res in ('y', 'Y', 'yes'):
            return True
        if res in ('n', 'N', 'no'):
            return False
