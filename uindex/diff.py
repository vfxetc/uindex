import argparse
import collections
import re

from .parse import iter_entries


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--prepend-a', '--pa')
    parser.add_argument('--prepend-b', '--pb')
    parser.add_argument('--search-a', '--sa')
    parser.add_argument('--search-b', '--sb')
    parser.add_argument('--replace-a', '--ra', nargs=2)
    parser.add_argument('--replace-b', '--rb', nargs=2)
    parser.add_argument('-v', '--invert-search', action='store_true')
    parser.add_argument('-L', '--ignore-links', action='count')
    parser.add_argument('a')
    parser.add_argument('b')
    args = parser.parse_args()

    match = missing = extra = 0

    print '---', args.a
    A = sorted(iter_entries(open(args.a),
        prepend_path=args.prepend_a,
        replace_path=args.replace_a,
        search_path=args.search_a,
        invert_search=args.invert_search,
    ), key=lambda x: x.path)

    print '+++', args.b
    B = sorted(iter_entries(open(args.b),
        prepend_path=args.prepend_b,
        replace_path=args.replace_b,
        search_path=args.search_b,
        invert_search=args.invert_search,
    ), key=lambda x: x.path)

    def pop(X):
        x = X.pop(0)
        while X and X[0].path == x.path:
            X.pop(0)
        return x

    last_link = None

    while A and B:

        a = A[0]
        b = B[0]

        ax = (a.path, a.checksum)
        bx = (b.path, b.checksum)

        if ax == bx:
            pop(A)
            pop(B)
            match += 1
            last_link = None

        elif ax < bx:
            if last_link  and a.path.startswith(last_link):
                if args.ignore_links > 1:
                    print ' ', a.checksum, a.path
                pop(A)
                match += 1
            else:
                print '-', a.checksum, a.path
                pop(A)
                missing += 1

        else:
            if b.type == '@' and args.ignore_links:
                last_link = b.path + '/'
                if args.ignore_links > 1:
                    print '@', b.checksum, b.path
                pop(B)
            else:
                print '+', b.checksum, b.path
                pop(B)
                extra += 1

    for a in A:
        print '-', a.checksum, a.path
        missing += 1
    for b in B:
        print '+', b.checksum, b.path
        extra += 1

    print '{} match, {} missing, {} extra.'.format(match, missing, extra)
    exit()



if __name__ == '__main__':
    main()

