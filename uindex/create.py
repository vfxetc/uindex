#!/usr/bin/env python

from __future__ import print_function

from Queue import Queue, Empty
from uuid import uuid4
import argparse
import datetime
import functools
import hashlib
import itertools
import json
import math
import os
import re
import stat
import sys
import threading
import time
import traceback

from .parse import iter_entries
from .utils import parse_bytes


# Stat times are nanoseconds underneath, but in Python 2 we
# get floats, which have only 53 bits of precision. Since
# time is seconds since the epoch, today we only have ~21.5 bits
# for subsecond (slightly more than 6 digits), and that will
# decrease as time goes on.
STAT_TIME_DIGITS = int((53 - math.log(int(time.time()), 2)) / math.log(10, 2))


# These types are from `zfs diff`.
REG = 'F'
LNK = '@'
DIR = '/'
# We don't really deal with below here TBH.
BLK = 'B' # Block device.
CHR = 'C' # Character device.
DOOR = '>' # Door.
FIFO = '|' # Named pipe.
SOCK = '=' # Socket.
PORT = 'P' # Event port.


class WalkItem(object):

    def __init__(self, parent, name):
        
        self.parent = parent
        self.name = name
        self.path = os.path.join(parent, name)
        
        self.stat = os.lstat(self.path)
        mode = self.stat.st_mode
        self.perms = stat.S_IMODE(mode)

        self.is_reg = self.is_dir = self.is_lnk = self.is_special = False
        if stat.S_ISREG(mode):
            self.is_reg = True
            self.type_code = REG
        elif stat.S_ISDIR(mode):
            self.is_dir = True
            self.type_code = DIR
        elif stat.S_ISLNK(mode):
            self.is_lnk = True
            self.type_code = LNK
        else:
            self.is_special = True
            self.type_code = None

    def __repr__(self):
        return 'WalkItem({!r}, {!r})'.format(self.parent, self.name)


def resumeable_walk(dir_, start=None):
    if start:
        start = os.path.relpath(os.path.join(dir_, start), dir_)
        start = start.split(os.path.sep)
    return _resumeable_walk(dir_, start)


def _resumeable_walk(dir_, start):

    if start:
        this_start = start[0]
        next_start = start[1:]
    else:
        this_start = next_start = None

    items = []

    names = sorted(os.listdir(dir_), key=str.lower)
    for name in names:

        if this_start and this_start > name:
            continue

        try:
            item = WalkItem(dir_, name)
        except Exception as e:
            printerr('# Exception in resumable walk:', e)
            raise

        if item.is_special:
            continue
        items.append(item)

    # Since files and dirs are yielded at the same time, files after
    # the start point will have already been processed, and will get
    # processed again unless we ignore this level entirely.
    if not next_start:
        yield items

    # The user is allowed to mutate the items.

    for item in items:

        if not item.is_dir:
            continue

        if this_start and item.name > this_start:
            next_start = None

        for x in _resumeable_walk(item.path, next_start):
            yield x



_checksum_cache = {}

def _checksum_path(item, indexer):

    algo_name = indexer.checksum_algo
    head = indexer.head
    tail = indexer.tail
    
    algo_key = algo_name
    if head:
        algo_key = '{},h={}'.format(algo_key, indexer.raw_head)
    if tail:
        algo_key = '{},t={}'.format(algo_key, indexer.raw_tail)

    # We cache every checksum by device/inode so we don't bother re-indexing things which
    # are hardlinked.
    st = item.stat
    cache_key = (st.st_dev, st.st_ino, algo_key)
    try:
        checksum, ctime = _checksum_cache[cache_key]
        if ctime == st.st_ctime:
            return to_index, checksum
    except KeyError:
        pass

    hasher = getattr(hashlib, algo_name)()

    if item.is_reg:

        with open(item.path, 'rb') as fh:
            
            loc = 0

            for todo, is_tail in ((head, False), (tail, True)):

                if is_tail:
                    if tail:
                        # Only do the remaining parts.
                        offset = max(loc, st.st_size - tail)
                        if offset == loc:
                            continue
                        fh.seek(offset)
                    else:
                        # Don't bother with the tail if it isn't requested.
                        continue

                chunksize = 65536
                while todo is None or todo > 0:
                    if todo:
                        chunksize = min(chunksize, todo)
                    try:
                        chunk = fh.read(chunksize)
                    except IOError as e:
                        # For some reason, some files in "System Volume Information"
                        # throw an error no matter what you do.
                        if e.errno != 1:
                            raise
                        return item, None
                    if not chunk:
                        break
                    if todo:
                        todo -= len(chunk)    
                    hasher.update(chunk)

                loc = fh.tell()

    elif item.is_lnk:
        path = os.readlink(item.path)
        hasher.update(path)

    checksum = '{}:{}'.format(algo_key, hasher.hexdigest())
    _checksum_cache[cache_key] = checksum, st.st_ctime
    return item, checksum


def _threaded_map(num_threads, func, *args_iters, **kwargs):

    sorted = kwargs.pop('sorted', True)
    strict = kwargs.pop('strict', False)

    work_queue = Queue(num_threads)
    result_queue = Queue()
    
    results = {}
    workers = []
    alive = 0

    scheduler = threading.Thread(target=_threaded_map_scheduler, args=(num_threads, work_queue, args_iters))
    scheduler.daemon = True
    scheduler.start()

    for _ in xrange(num_threads):
        worker = threading.Thread(target=_threaded_map_target, args=(work_queue, result_queue, func))
        worker.daemon = True
        worker.start()
        workers.append(worker)
        alive += 1

    next_job = 0
    while alive:
        
        job, ok, result = result_queue.get()
        if job is None:
            alive -= 1
            continue

        if not sorted:
            if ok:
                yield result
            else:
                raise result
            continue

        results[job] = (ok, result)

        while next_job in results:
            ok, result = results.pop(next_job)
            if ok:
                yield result
            elif strict:
                raise result
            next_job += 1

    for worker in workers:
        worker.join(0.1)
        if worker.is_alive():
            raise ValueError('Worker survived.')


def _threaded_map_scheduler(num_threads, work_queue, args_iters):
    try:
        for i, args in enumerate(itertools.izip(*args_iters)):
            work_queue.put((i, args))
    except Exception as e:
        traceback.print_exc()
        raise
    finally:
        for _ in xrange(num_threads):
            work_queue.put((None, None))

def _threaded_map_target(work_queue, result_queue, func):
    try:
        while True:
            job, args = work_queue.get()
            if args is None:
                break
            try:
                result = func(*args)
            except Exception as e:
                printerr('# Exception during {}{}'.format(func.__name__, args))
                result = e
                ok = False
            else:
                ok = True
            result_queue.put((job, ok, result))
    except Exception as e:
        traceback.print_exc()
        raise
    finally:
        result_queue.put((None, None, None))



class Indexer(object):

    def __init__(self, path_to_index, root=None, start=None, excludes=(),
        include_dotfiles=False, head=None, tail=None, checksum_algo='sha256', verbosity=0):

        self.path_to_index = os.path.abspath(path_to_index)
        self.root = os.path.abspath(root or self.path_to_index)
        self.start = start
        self.verbosity = int(verbosity)
        self.checksum_algo = checksum_algo

        self.raw_head = head
        self.head = parse_bytes(head) if head else None
        self.raw_tail = tail
        self.tail = parse_bytes(tail) if tail else None

        self.raw_excludes = excludes
        self.name_excludes = []
        self.path_excludes = []

        for raw in excludes:
            if raw.startswith('/'):
                self.path_excludes.append(re.compile(r'^%s$' % raw.strip('/')))
            else:
                self.name_excludes.append(re.compile(r'^%s$' % raw.strip('/')))
        if not include_dotfiles:
            self.name_excludes.append(re.compile(r'^\.'))

        self.existing = {}

    def auto_start(self, index_path):
        with open(index_path, 'rb') as out:
            out.seek(-1000, 2)
            last = out.read().splitlines()[-1]
            rel_start = last.split('\t')[-1]
            self.start = os.path.join(self.root, rel_start)

    def load_existing(self, input_):
        if isinstance(input_, basestring):
            input_ = open(input_, 'rb')
        for entry in iter_entries(input_):
            self.existing[entry.path] = entry


    def _iter_file_paths(self):

        # Early-binding fo speed.
        path_excludes = self.path_excludes
        name_excludes = self.name_excludes
        existing = self.existing
        root = self.root
        S_ISREG = stat.S_ISREG

        self.added_count = added_count = 0
        self.added_bytes = added_bytes = 0
        self.total_count = total_count = 0
        self.total_bytes = total_bytes = 0

        for items in resumeable_walk(self.path_to_index, self.start):

            if path_excludes or name_excludes:

                i = 0
                while i < len(items):

                    item = items[i]
                    name = item.name
                    exclude = False

                    if name_excludes and any(r.match(name) for r in name_excludes):
                        exclude = True

                    elif path_excludes:
                        rel_path = os.path.relpath(os.path.join(dir_path, name), root)
                        if any(r.match(rel_path) for r in path_excludes):
                            exclude = True

                    if exclude:
                        items.pop(i)
                    else:
                        i += 1

            for item in items:

                abs_path = item.path
                rel_path = item.rel_path = os.path.relpath(abs_path, root)
                st = item.stat

                # We only care about actual files.
                if not (item.is_reg or item.is_lnk):
                    continue

                total_count += 1
                total_bytes += st.st_size

                entry = existing.get(rel_path)
                if entry:
                    # When writing, we used to round the mtime to 0.01, so we have to
                    # do a fuzzy compare.
                    # TODO: Should we be checking ctime or mtime here?
                    if entry.size == st.st_size and abs(entry.mtime - st.st_mtime) < entry.epsilon:
                        if self.verbosity > 1:
                            printerr("# Skipping unchanged {}".format(rel_path))
                        continue
                    elif self.verbosity > 1:
                        printerr("# Reindexing changed {}".format(rel_path))
                
                elif self.verbosity > 1:
                    printerr("# Indexing new {}".format(rel_path))

                added_count += 1
                added_bytes += st.st_size

                yield item

        self.added_count = added_count
        self.added_bytes = added_bytes
        self.total_count = total_count
        self.total_bytes = total_bytes

    def run(self, out, threads=1, sorted=True, header_extra=None):

        self.error_count = 0

        uuid = str(uuid4())

        header = dict(header_extra or {})
        header.update(
            path_to_index=self.path_to_index,
            root=self.root,
            start=self.start,
            started_at=datetime.datetime.utcnow().isoformat('T'),
            uuid=uuid,
            excludes=self.raw_excludes,
            checksum_algo=self.checksum_algo,
            head=self.head,
            tail=self.tail,
            columns='''
                checksum
                inode
                type
                perms
                size
                uid
                gid
                mtime
                ctime
                path
            '''.strip().split()
        )

        out.write('#scan-start {}\n'.format(json.dumps(header, sort_keys=True)))

        last_flush = time.time()

        for item, checksum in _threaded_map(
            threads,
            _checksum_path,
            self._iter_file_paths(),
            itertools.cycle((self, )),
            sorted=sorted,
        ):

            # Sometimes there are wierd errors.
            if not checksum:
                out.write('#scan-error {}\n'.format(json.dumps(dict(path=item.rel_path))))
                self.error_count += 1
                continue

            formatted = '\t'.join(str(x) for x in (
                checksum,
                item.stat.st_ino,
                item.type_code,
                '{:o}'.format(item.perms),
                item.stat.st_size,
                item.stat.st_uid,
                item.stat.st_gid,

                '{:.{}f}'.format(item.stat.st_mtime, STAT_TIME_DIGITS),
                '{:.{}f}'.format(item.stat.st_ctime, STAT_TIME_DIGITS),

                item.rel_path,
            ))
            if self.verbosity:
                print(formatted)
            out.write(formatted + '\n')

            now = time.time()
            if now - last_flush > 1:
                out.flush()
                last_flush = now

        out.write('#scan-end {}\n'.format(json.dumps(dict(
            added_count=self.added_count,
            added_bytes=self.added_bytes,
            total_count=self.total_count,
            total_bytes=self.total_bytes,
            error_count=self.error_count,
            ended_at=datetime.datetime.utcnow().isoformat('T'),
            uuid=uuid,
        ), sort_keys=True)))








def printerr(*args, **kwargs):
    kwargs['file'] = sys.stderr
    print(*args, **kwargs)


def main(argv=None):

    parser = argparse.ArgumentParser()
    
    parser.add_argument('-D', '--include-dotfiles', action='store_true',
        help="Don't exclude files that start with dots.")

    parser.add_argument('-e', '--exclude', action='append', default=[],
        help="Exclude files that match this regex; can be used multiple times.")

    parser.add_argument('-o', '--out',
        help="File to write to instead of stdout.")

    parser.add_argument('-s', '--start', type=os.path.abspath,
        help="A path to re-start indexing from.")
    parser.add_argument('-S', '--auto-start', action='store_true',
        help="Automatically re-start at the last element in the index.")
    parser.add_argument('-u', '--update', action='store_true',
        help="Update index with files that were missing or changed from last run.")

    parser.add_argument('--unsorted', action='store_true',
        help="Will lose less work if there is a crash, but --auto-start will skip over any lost work.")

    parser.add_argument('--head',
        help="How much of front of file to checksum.")
    parser.add_argument('--tail',
        help="How much of end of file to checksum.")

    parser.add_argument('-t', '--threads', type=int, default=1,
        help="How many threads to run at once.")
    parser.add_argument('-H', '--checksum-algo', default='sha256',
        help="Which hashlib algorithm to use.")

    parser.add_argument('-C', '--root', type=os.path.abspath,
        help="Root from which relative paths will be derived.")

    parser.add_argument('-v', '--verbose', action='count', default=0,
        help="Print out progress even if redirecting to --out.")

    parser.add_argument('path', type=os.path.abspath)

    args = parser.parse_args(argv)

    def feedback(*a, **kwargs):
        if args.verbose:
            printerr(*a, **kwargs)

    if sum(map(bool, (args.auto_start, args.start, args.update))) > 1:
        printerr("--start, --auto-start, and --update don't work together.")
        exit(1)
    if args.auto_start and not args.out:
        printerr("--auto-start requires --out.")
        exit(2)
    if args.update and not args.out:
        printerr("--update requires --out.")
        exit(2)

    indexer = Indexer(
        path_to_index=args.path,
        root=args.root,
        start=args.start,
        excludes=args.exclude,
        include_dotfiles=args.include_dotfiles,
        checksum_algo=args.checksum_algo,
        head=args.head,
        tail=args.tail,
        verbosity=args.verbose,
    )

    if args.auto_start:
        if not os.path.exists(args.out):
            printerr("Output file must exist for --auto-start.")
            exit(3)
        indexer.auto_start(args.out)
        feedback('Restarting at', indexer.start)

    if args.update:
        if not os.path.exists(args.out):
            printerr("Output file must exist for --update.")
            exit(3)
        feedback("Reading existing index to update...")
        indexer.load_existing(args.out)

    out = open(args.out, 'ab' if (args.start or args.auto_start or args.update) else 'wb') if args.out else sys.stdout
    indexer.run(out,
        threads=args.threads,
        sorted=not args.unsorted,
        header_extra=dict(
            cli=dict(
                argv=sys.argv,
                cwd=os.getcwd(),
            )
        ),
    )


if __name__ == '__main__':
    exit(main())
