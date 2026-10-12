"""Hadoop Streaming reducer for shuffled date keys."""
import sys
from itertools import groupby
for date, rows in groupby(sys.stdin, key=lambda line: line.split('\t', 1)[0]):
    energy = 0.0; count = 0
    for line in rows:
        _, e, c = line.strip().split('\t')
        energy += float(e); count += int(c)
    print(f'{date}\t{energy:.9f}\t{count}')
