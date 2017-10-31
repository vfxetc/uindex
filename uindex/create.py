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

    for name in sorted(os.listdir(dir_)):

        if this_start and this_start > name:
            continue
        if this_start and name > this_start:
            next_start = None

        if os.path.isdir(os.path.join(dir_, name)):
            dirs.append(name)
        else:
            non_dirs.append(name)

    yield dir_, dirs, non_dirs

    for name in dirs:
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


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--dots', action='store_true')
    parser.add_argument('-e', '--exclude', action='append', default=[])
    parser.add_argument('-o', '--out')
    parser.add_argument('-s', '--start')
    parser.add_argument('-t', '--threads', type=int, default=1)
    parser.add_argument('-C', '--root', default=os.getcwd())
    parser.add_argument('path')

    args = parser.parse_args()

    out = open(args.out, 'wb') if args.out else sys.stdout

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

        out.write('\t'.join(str(x) for x in (
            checksum,
            '{:o}'.format(st.st_mode & 0o7777),
            st.st_size,
            st.st_uid,
            st.st_gid,
            '{:.2f}'.format(st.st_mtime),
            rel_path,
        )) + '\n')

if __name__ == '__main__':
    exit(main())
