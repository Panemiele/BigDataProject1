#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN (standard input)
for line in sys.stdin:

    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into words
    words = line.split(" ")

    for word in words:
        # write in standard output the mappings word -> 1
        # in the form of tab-separated pairs
        print('%s\t%i' % (word, 1))
