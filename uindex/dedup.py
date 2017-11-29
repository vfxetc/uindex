import argparse
import os

from .utils import iter_raw_index


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-n', '--dry-run', action='store_true')
    parser.add_argument('--delete-from', type=os.path.abspath)
    parser.add_argument('--apop', type=int)
    parser.add_argument('--bpop', type=int)
    parser.add_argument('a')
    parser.add_argument('b')
    args = parser.parse_args()

    print 'Loading', args.a
    checksum_to_paths = {}
    for token in iter_raw_index(open(args.a)):
        path = token.path
        if args.apop:
            path = path.split('/', args.apop)[-1]
        checksum_to_paths.setdefault(token.checksum, set()).add(path)

    duped_bytes = 0

    print 'Scanning', args.b
    for token in iter_raw_index(open(args.b)):

        path = token.path
        if args.bpop:
            path = path.split('/', args.bpop)[-1]

        existing = checksum_to_paths.get(token.checksum)
        if existing:
            duped_bytes += token.size
            if path in existing:
                
                if args.verbose:
                    print '{}G; {} in both at: {}'.format(duped_bytes / (1024**3), token.checksum, path)

                if args.delete_from:
                    abspath = os.path.join(args.delete_from, path)
                    if os.path.exists(abspath):
                        print '\t$ rm', abspath
                        if not args.dry_run:
                            os.unlink(abspath)

            else:
                print '{}G; {} in B in {} locations:\n\tA: {}\n\tB: {}'.format(duped_bytes / (1024**3), token.checksum, len(existing), path, sorted(existing)[0])


if __name__ == '__main__':
    main()
