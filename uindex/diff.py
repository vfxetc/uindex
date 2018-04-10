import argparse
import heapq
import collections

from .utils import iter_raw_index





def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--prepend-a')
    parser.add_argument('--prepend-b')
    parser.add_argument('a')
    parser.add_argument('b')
    args = parser.parse_args()

    match = missing = extra = 0

    print '---', args.a
    A = sorted(iter_raw_index(open(args.a), prepend_path=args.prepend_a), key=lambda x: x.path)
    print '+++', args.b
    B = sorted(iter_raw_index(open(args.b), prepend_path=args.prepend_b), key=lambda x: x.path)

    def pop(X):
        x = X.pop(0)
        while X and X[0].path == x.path:
            X.pop(0)
        return x

    while A and B:

        a = A[0]
        b = B[0]

        ax = (a.path, a.checksum)
        bx = (b.path, b.checksum)

        if ax == bx:
            pop(A)
            pop(B)
            match += 1

        elif ax < bx:
            print '-', a.checksum, a.path
            pop(A)
            missing += 1

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

