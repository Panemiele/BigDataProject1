#!/usr/bin/env python3
"""reducer.py"""

import sys

# this dictionary maps each word to the sum of the values
# that the mapper has computed for that word
word_2_sum = {}

# input comes from STDIN
# note: this is the output from the mapper!
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    current_word, current_count = line.split("\t")

    # convert count (currently a string) to int
    try:
        current_count = int(current_count)
    except ValueError:
        # count was not a number, so
        # silently ignore/discard this line
        continue

    # initialize words that were not seen before with 0
    if current_word not in word_2_sum:
        word_2_sum[current_word] = 0

    word_2_sum[current_word] += current_count

for word in word_2_sum:
    print("%s\t%i" % (word, word_2_sum[word]))
