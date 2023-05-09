#!/usr/bin/python
"""mapper.py"""

import sys

# Funzione che restituisce una tripla (user_id, num, denom) a partire da una riga del file CSV
def parse_input_line(line):
    row = line.strip().split(',')
    user_id, num, denom = row[2], row[3], row[4]
    return (user_id, num, denom)

# read lines from STDIN (standard input)
for line in sys.stdin:
    (user_id, num, denom) = parse_input_line(line)
    print('%s,%i,%i' % (user_id, num, denom))
