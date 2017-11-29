import argparse
import heapq
import collections

from .utils import iter_raw_index





def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('a')
    parser.add_argument('b')
    args = parser.parse_args()

    missing = extra = 0

    print '---', args.a
    A = sorted(iter_raw_index(open(args.a)), key=lambda x: x.path)
    print '+++', args.b
    B = sorted(iter_raw_index(open(args.b)), key=lambda x: x.path)

    def pop(X):
        x = X.pop(0)
        while X and X[0].path == x.path:
            X.pop(0)
        return x

    while A and B:

        a = A[0]
        b = B[0]

        if a == b:
            pop(A)
            pop(B)

        elif a < b:
            print '-', a.checksum, a.path
            pop(A)
            missing += 1

        else:
            print '+', b.checksum, b.path
            pop(B)
            extra += 1

    for a in A:
        print '-', a.checksum, a.path
    for b in B:
        print '-', b.checksum, b.path

    print '{} missing, {} extra.'.format(missing, extra)
    exit()



if __name__ == '__main__':
    main()
