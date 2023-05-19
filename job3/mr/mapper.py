#!/usr/bin/python
"""mapper.py"""
import datetime
import sys

def parse_input_line(line):
    row = line.strip().split(',')
    prod_id, user_id, score = row[1], row[2], int(row[5])
    return prod_id, user_id, score

# Salta gli header del dataset
sys.stdin.readline()
# Leggi le righe dallo standard input
for line in sys.stdin:
    try:
        prod_id, user_id, score = parse_input_line(line)
        if score >= 4:
            print('%s\t%s' % (user_id, prod_id))
    except ValueError:
        continue
