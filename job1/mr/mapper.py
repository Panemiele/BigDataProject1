#!/usr/bin/env python3
"""mapper.py"""
import datetime
import sys

# Funzione che restituisce una tupla (anno, (prod_id, testo)) a partire da una riga del file CSV
def parse_input_line(line):
    row = line.strip().split(',')
    prod_id, time, text = row[1], row[6], row[7]
    year = datetime.utcfromtimestamp(time).strftime('%Y')
    return (year, (prod_id, text))

# Leggi le righe dallo standard input
for line in sys.stdin:
    try:
        year, (prod_id, text) = parse_input_line(line)
    except:
        continue
    # Emit la coppia chiave-valore (anno_prod_id, testo) per ogni prodotto
    print(f"{year}_{prod_id},{text}")
