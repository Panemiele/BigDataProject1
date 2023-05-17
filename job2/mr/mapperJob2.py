#!/usr/bin/python
"""mapperJob2.py"""

import sys

# Funzione che restituisce una tripla (user_id, num, denom) a partire da una riga del file CSV
def parse_input_line(line):
    row = line.strip().split(',')
    user_id, num, denom = row[2], row[3], row[4]
    return (user_id, num, denom)


# Salta gli header del dataset
sys.stdin.readline()
# Leggi le righe dallo standard input
for line in sys.stdin:
    (user_id, num, denom) = parse_input_line(line)
    print("%s,%s,%s" % (user_id, num, denom))
