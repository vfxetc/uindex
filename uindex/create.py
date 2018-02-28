#!/usr/bin/env python

from Queue import Queue, Empty
import argparse
import datetime
import functools
import hashlib
import itertools
import os
import re
import sys
import threading



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

    dirs = []
    non_dirs = []

    names = sorted(os.listdir(dir_))
    for name in names:

        if this_start and this_start > name:
            continue

        # We skip over things which are not directories or files.
        path = os.path.join(dir_, name)
        if os.path.isdir(path):
            dirs.append(name)
        elif os.path.isfile(path):
            non_dirs.append(name)

    # Since files and dirs are yielded at the same time, files after
    # the start point will have already been processed, and will get
    # processed again unless we ignore this level entirely.
    if not next_start:
        yield dir_, dirs, non_dirs

    for name in dirs:

        if this_start and name > this_start:
            next_start = None

        for x in _resumeable_walk(os.path.join(dir_, name), next_start):
            yield x


def _iter_file_paths(root, start, path_excludes, name_excludes):

    for dir_path, dir_names, file_names in resumeable_walk(root, start):

        if path_excludes or name_excludes:
            for names in (dir_names, file_names):

                i = 0
                while i < len(names):

                    name = names[i]
                    exclude = False

                    if name_excludes and any(r.match(name) for r in name_excludes):
                        exclude = True

                    elif path_excludes:
                        rel_path = os.path.relpath(os.path.join(dir_path, name), root)
                        if any(r.match(rel_path) for r in path_excludes):
                            exclude = True

                    if exclude:
                        names.pop(i)
                    else:
                        i += 1

        for name in file_names:
            path = os.path.join(dir_path, name)
            yield path


def _checksum_path(path):
    hasher = hashlib.sha256()
    with open(path, 'rb') as fh:
        while True:
            try:
                chunk = fh.read(65536)
            except IOError as e:
                # For some reason, some files in "System Volume Information"
                # throw an error no matter what you do.
                if e.errno != 1:
                    raise
                return path, None
            if not chunk:
                break
            hasher.update(chunk)
    return path, hasher.hexdigest()


def _threaded_map(num_threads, func, *args_iters, **kwargs):

    sorted = kwargs.pop('sorted', True)

    work_queue = Queue(num_threads)
    result_queue = Queue()
    
    results = {}
    workers = []
    alive = 0

    scheduler = threading.Thread(target=_threaded_map_scheduler, args=(work_queue, args_iters))
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
            else:
                raise result
            next_job += 1

    for worker in workers:
        if worker.is_alive():
            raise ValueError('Worker survived.')


def _threaded_map_scheduler(work_queue, args_iters):
    try:
        for i, args in enumerate(itertools.izip(*args_iters)):
            work_queue.put((i, args))
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
                result = e
                ok = False
            else:
                ok = True
            result_queue.put((job, ok, result))
    finally:
        result_queue.put((None, None, None))



def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--dots', action='store_true')
    parser.add_argument('-e', '--exclude', action='append', default=[])
    parser.add_argument('-o', '--out')
    parser.add_argument('-s', '--start', type=os.path.abspath)
    parser.add_argument('-S', '--auto-start', action='store_true')
    parser.add_argument('--sorted', action='store_true', help='Slower, but in order.')
    parser.add_argument('-t', '--threads', type=int, default=1)
    parser.add_argument('-C', '--root', type=os.path.abspath)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('path', type=os.path.abspath)

    args = parser.parse_args(argv)

    if args.auto_start and args.start:
        print >> sys.stderr, "--start and --auto-start don't work together."
        exit(1)
    if args.auto_start and not args.out:
        print >> sys.stderr, "--auto-start requires --out."
        exit(2)

    if args.root is None:
        args.root = args.path

    if args.auto_start:
        if os.path.exists(args.out):
            with open(args.out, 'rb') as out:
                out.seek(-1000, 2)
                last = out.read().splitlines()[-1]
                rel_start = last.split('\t')[-1]
                args.start = os.path.join(args.root, rel_start)
                if args.verbose:
                    print 'Restarting at', args.start
        elif args.verbose:
            print >> sys.stderr, 'Output does not exist for --auto-start.'

    out = open(args.out, 'ab' if (args.start or args.auto_start) else 'wb') if args.out else sys.stdout

    path_excludes = []
    name_excludes = []
    for raw in args.exclude:
        if raw.startswith('/'):
            path_excludes.append(re.compile(r'^%s$' % raw.strip('/')))
        else:
            name_excludes.append(re.compile(r'^%s$' % raw.strip('/')))
    if not args.dots:
        name_excludes.append(re.compile(r'^\.'))

    for abs_path, checksum in _threaded_map(args.threads, _checksum_path, _iter_file_paths(args.path, args.start, path_excludes, name_excludes), sorted=args.sorted):

        # Sometimes there are wierd errors.
        if not checksum:
            continue

        rel_path = os.path.relpath(abs_path, args.root)
        st = os.stat(abs_path)

        formatted = '\t'.join(str(x) for x in (
            checksum,
            '{:o}'.format(st.st_mode & 0o7777),
            st.st_size,
            st.st_uid,
            st.st_gid,
            '{:.2f}'.format(st.st_mtime),
            rel_path,
        ))
        if args.verbose:
            print formatted
        out.write(formatted + '\n')

if __name__ == '__main__':
    exit(main())
