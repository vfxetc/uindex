import collections


Token = collections.namedtuple('Token', ['path', 'checksum', 'perms', 'size', 'uid', 'gid', 'mtime'])

def iter_raw_index(fh):
    for line_i, line in enumerate(fh):
        line = line.strip()
        if not line:
            continue
        checksum, perms, size, uid, gid, mtime, path = line.split('\t')
        yield Token(path, checksum, int(perms), int(size), int(uid), int(gid), float(mtime))