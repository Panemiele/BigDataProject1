#!/usr/bin/python
"""mapper.py"""
import datetime
import sys

# Funzione che restituisce una tupla (anno, (prod_id, testo)) a partire da una riga del file CSV
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
    except ValueError:
        continue
    print("%s,%s,%i" % (user_id, prod_id, score))
