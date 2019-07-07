import re

from .utils import cached_property


class Entry(object):

    def __init__(self, path, checksum, perms, size, uid, gid, mtime, meta, ctime=None, inode=None):

        self.meta = dict(meta or {})

        self.path = path
        self.raw_checksum = checksum
        self.perms = int(perms, 8)
        self.size  = int(size)
        self.uid   = int(uid)
        self.gid   = int(gid)
        self.mtime = float(mtime)
        self.ctime = float(ctime) if ctime else None
        self.inode = int(inode) if inode else None

        self.raw_time = ctime or mtime

    @cached_property
    def checksum(self):
        return self.raw_checksum.split(':')[-1]

    @cached_property
    def epsilon(self):
        try:
            digits = self._raw_time.split('.')[1]
        except IndexError:
            return 0
        return 2 * 10 ** -digits

    def prepend_path(self, prefix):
        if prefix:
            self.path = '/'.join((prefix.strip('/'), self.path))

    def pop_path(self, num):
        self.path = self.path.split('/', num)[-1]

    def replace_path(self, pattern, replace):
        self.path = re.sub(pattern, replace, self.path)

    def search_path(self, pattern):
        return bool(re.search(pattern, self.path))
    
