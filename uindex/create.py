#!/usr/bin/env python

import argparse
import datetime
import functools
import os
import hashlib
import sys
import re
import itertools

from concurrent.futures import ThreadPoolExecutor



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

        if os.path.isdir(os.path.join(dir_, name)):
            dirs.append(name)
        else:
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
            chunk = fh.read(65536)
            if not chunk:
                break
            hasher.update(chunk)
    return path, hasher.hexdigest()


def _threaded_map(num_threads, func, *iterables):
    executor = ThreadPoolExecutor(num_threads)
    futures = []
    for args in itertools.izip(*iterables):
        futures.append(executor.submit(func, *args))
        if len(futures) > num_threads:
            yield futures.pop(0).result()
    for f in futures:
        yield f.result()


def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--dots', action='store_true')
    parser.add_argument('-e', '--exclude', action='append', default=[])
    parser.add_argument('-o', '--out')
    parser.add_argument('-s', '--start')
    parser.add_argument('-S', '--auto-start', action='store_true')
    parser.add_argument('-t', '--threads', type=int, default=1)
    parser.add_argument('-C', '--root', default=os.getcwd())
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('path')

    args = parser.parse_args(argv)

    if args.auto_start and args.start:
        print >> sys.stderr, "--start and --auto-start don't work together."
        exit(1)
    if args.auto_start and not args.out:
        print >> sys.stderr, "--auto-start requires --out."
        exit(2)
    if args.auto_start and not os.path.exists(args.out):
        print >> sys.stderr, "--auto-start requires --out to exist."
        exit(3)


    if args.auto_start:
        with open(args.out, 'rb') as out:
            out.seek(-1000, 2)
            last = out.read().splitlines()[-1]
            rel_start = last.split('\t')[-1]
            args.start = os.path.join(args.root, rel_start)
            if args.verbose:
                print 'Restarting at', args.start

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

    args.root = os.path.abspath(args.root)
    args.path = os.path.abspath(args.path)

    for abs_path, checksum in _threaded_map(args.threads, _checksum_path, _iter_file_paths(args.path, args.start, path_excludes, name_excludes)):

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
