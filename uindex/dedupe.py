from __future__ import print_function

import argparse
import os

from .utils import prompt_bool, format_bytes, parse_bytes
from .parse import iter_entries


def iter_relpaths(path):
    chunks = path.split('/')
    for i in xrange(len(chunks)):
        yield '/'.join(chunks[i:])


def main():

    parser = argparse.ArgumentParser()
    
    parser.add_argument('-v', '--verbose', action='count', default=0)

    parser.add_argument('-n', '--dry-run', action='store_true',
        help="Always say no.")
    parser.add_argument('-y', '--yes', action='store_true',
        help="Always say yes.")
    
    parser.add_argument('-C', '--root', type=os.path.abspath, default=os.getcwd(),
        help="The root to manipulate files in.")
    
    # internal_args = parser.add_argument_group('Dedupe internal')
    # internal_args.add_argument('-H', '--link-self', action='store_true',
    #     help="Hardlink together matching files.")

    external_args = parser.add_argument_group('Dedupe external',
        description="Delete files here that match an external index. --delete-matching triggers this mode.")
    external_args.add_argument('-d', '--delete-matching',
        help="Delete files in our root that also exist in this index.")

    external_args.add_argument('-p', '--pop-path', metavar="NUM", type=int,
        help="Segments to pop off front of paths before matching.")
    external_args.add_argument('-P', '--prepend-path',
        help="Prefix to add in front of paths before matching.")

    external_args.add_argument('-U', '--match-unique-relpath', action='store_true',
        help="Relax matching so that only a relative path must be unique. It is "
             "possible that only the name of the file matches, as long as it is unique.")
    external_args.add_argument('-N', '--match-name', action='store_true',
        help="Relax matching so that names must match, but they need not be unique.")
    external_args.add_argument('--match-checksum', action='store_true',
        help="Relax matching so that names need not match at all.")

    external_args.add_argument('-S', '--minsize', metavar="SIZE", type=parse_bytes,
        help="Tighten matching so that file size is at least this large.")

    parser.add_argument('index')

    args = parser.parse_args()

    def verbose(lvl, *a, **kwargs):
        if args.verbose >= lvl:
            print(*a, **kwargs)

    verbose(1, 'Loading', args.index)

    bytes_ = 0
    dupes = 0

    by_checksum = {}
    for entry in iter_entries(open(args.index), pop_path=args.pop_path, prepend_path=args.prepend_path):
        if entry.checksum in by_checksum:
            bytes_ += entry.size
            dupes += 1
        by_checksum.setdefault((entry.checksum, entry.size), []).append(entry)
    
    verbose(1, '{} internal dupes (by checksum) across {} files.'.format(format_bytes(bytes_), dupes))

    if args.delete_matching:

        bytes_ = 0

        for entry in iter_entries(open(args.delete_matching)):

            self_entries = by_checksum.get((entry.checksum, entry.size))
            if not self_entries:
                continue

            path = entry.path

            # Check the matching conditions, from most to least relaxed.
            

            if args.match_checksum:
                matches = self_entries[:]

            elif args.match_name:
                name = os.path.basename(path)
                matches = [e for e in self_entries if os.path.basename(e.path) == name]

            if args.match_unique_relpath:
                
                by_relpath = {}
                for e in self_entries:
                    for relpath in iter_relpaths(e.path):
                        by_relpath.setdefault(relpath, []).append(e)

                matches = set()
                for relpath in iter_relpaths(entry.path):
                    entries = by_relpath.get(relpath)
                    if entries and len(entries) == 1:
                        matches.add(entries[0])
                matches = list(matches)

            else:
                matches = [e for e in self_entries if path == e.path]

            if args.minsize:
                matches = [e for e in matches if e.size >= args.minsize]

            if len(matches) != len(self_entries):
                print('{} in both at {} non-matching paths(s) (of {}):'.format(entry.checksum, len(self_entries) - len(matches), len(self_entries)))
                print('\tint: {}'.format(path))
                print('\text: {}'.format(sorted(e.path for e in self_entries if e not in matches)[0]))

            for match in matches:

                bytes_ += entry.size

                verbose(1, '{}G; {} at {}'.format(bytes_ / (1024**3), entry.checksum, match.path))
                abspath = os.path.join(args.root, match.path)

                if os.path.exists(abspath):
                    if args.yes or (args.verbose and args.dry_run) or prompt_bool("Delete {}?".format(abspath)):
                        if args.verbose:
                            print('\t$ rm', abspath)
                        if not args.dry_run:
                            os.unlink(abspath)
                else:
                    verbose(1, 'Cannot find local file:\n\t{}'.format(abspath))



if __name__ == '__main__':
    main()
