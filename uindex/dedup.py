import argparse
import os

from .utils import iter_raw_index, prompt_bool


def main():

    parser = argparse.ArgumentParser()
    
    parser.add_argument('-v', '--verbose', action='store_true')

    parser.add_argument('-n', '--dry-run', action='store_true',
        help="Always say no.")
    parser.add_argument('-y', '--yes', action='store_true',
        help="Always say yes.")
    
    parser.add_argument('-C', '--root', type=os.path.abspath,
        help="The root to manipulate files in.")
    
    parser.add_argument('-d', '--delete-matching',
        help="Delete files in our root that also exist in this index.")
    parser.add_argument('-H', '--link-self', action='store_true')

    parser.add_argument('-p', '--pop', type=int,
        help="How many segments to pop off front of paths.")
    parser.add_argument('-P', '--prefix',
        help="Prefix to add in front of paths.")
    parser.add_argument('-N', '--match-by-name', action='store_true')

    parser.add_argument('index')

    args = parser.parse_args()

    if args.verbose:
        print 'Loading', args.index

    bytes_ = 0
    dupes = 0

    checksum_to_paths = {}
    for token in iter_raw_index(open(args.index)):
        path = token.path
        if args.pop:
            path = path.split('/', args.pop)[-1]
        if token.checksum in checksum_to_paths:
            bytes_ += token.size
            dupes += 1
        checksum_to_paths.setdefault(token.checksum, set()).add(path)

    if args.verbose:
        print '{}G duplicated across {} files.'.format(bytes_ / 1024**3, dupes)

    if args.delete_matching:

        bytes_ = 0

        for token in iter_raw_index(open(args.delete_matching)):

            existing_paths = checksum_to_paths.get(token.checksum)
            if not existing_paths:
                continue
            
            path = token.path

            if args.match_by_name:
                existing_names = set(os.path.basename(x) for x in existing_paths)
                name = os.path.basename(path)
                exists = name in existing_names
            else:
                exists = path in existing_paths

            if not exists:
                print '{} in both at {} different names(s):'.format(token.checksum, len(existing_paths))
                print '\t{}'.format(path)
                print '\t{}'.format(sorted(existing_paths)[0])
                continue
            
            bytes_ += token.size

            if args.verbose:
                print '{}G; {} in both at {}'.format(bytes_ / (1024**3), token.checksum, path)

            if args.root:
                abspath = os.path.join(args.root, path)
            else:
                abspath = os.path.abspath(path)

            if os.path.exists(abspath):
                if args.yes or (args.verbose and args.dry_run) or prompt_bool("Delete {}?".format(abspath)):
                    if args.verbose:
                        print '\t$ rm', abspath
                    if not args.dry_run:
                        os.unlink(abspath)



if __name__ == '__main__':
    main()
