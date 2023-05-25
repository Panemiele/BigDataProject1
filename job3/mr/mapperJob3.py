#!/usr/bin/python
"""mapperJob3.py"""
import sys
import time


def parse_input_line(line):
    row = line.strip().split(',')
    prod_id, user_id, score = row[1], row[2], int(row[5])
    return prod_id, user_id, score

start_time = time.time()

# Salta gli header del dataset
sys.stdin.readline()
# Leggi le righe dallo standard input
for line in sys.stdin:
    try:
        prod_id, user_id, score = parse_input_line(line)
        if score >= 4:
            print('%s\t%s__#__%s' % (user_id, prod_id, start_time))
    except ValueError:
        continue
