#!/usr/bin/env python

"""Checks max delta error between two PageRank outputs."""

import os, sys
import argparse, itertools

parser = argparse.ArgumentParser(description='Checks max delta error between two PageRank outputs.')
parser.add_argument('log1', type=str, help='First PageRank output file')
parser.add_argument('log2', type=str, help='Second PageRank output file')

f1 = parser.parse_args().log1
f2 = parser.parse_args().log2

pr1 = open(f1, 'rt')  # t is text mode, which is default
pr2 = open(f2, 'rt')

# max error
err1 = 0
id1 = 0

# next largest error
err2 = 0
id2 = 0

# L1-norm
norm = 0

# izip loads things into memory as needed
for r1,r2 in itertools.izip(pr1, pr2):
    v1 = float(r1.split()[1])
    v2 = float(r2.split()[1])
    newerr = abs(v1 - v2)
    norm += newerr
    if (newerr > err1):
        id2, err2 = id1, err1
        id1, err1 = int(r1.split()[0]), newerr

print("id %8i with %2.20f" % (id1, err1))
print("id %8i with %2.20f" % (id2, err2))
print("l1-norm %10.20f" % norm)
