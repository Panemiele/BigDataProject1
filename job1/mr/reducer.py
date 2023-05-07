#!/usr/bin/env python3
"""reducer.py"""

import sys
import heapq
import string
from collections import defaultdict


### PSEUDOCODICE
# Input: {year}_{prod_id},{text}
#
# Per ogni anno:
#   Contare il numero di prodotti che hanno avuto il maggior numero di recensioni (occorrenze maggiori nell'anno E NON IN GENERALE NEGLI ANNI)
#   Selezionare i 10 prodotti col numero maggiore di recensioni
#   Per ognuno di questi 10 prodotti
#       Contare il numero di parole con almeno 4 caratteri (length >= 4)
#       Selezionare le 5 parole col numero maggiore di occorrenze ***MOSTRARE IL NUMERO DI TALI OCCORRENZE***