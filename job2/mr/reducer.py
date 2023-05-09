#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections
import itertools

########################
# VARIABLE DEFINITIONS #
########################
user_num_and_denom_list = []
user_to_count_infos = {}
result = {}


#############
# FUNCTIONS #
#############


#######################################
# Leggi le righe dallo standard input #
#######################################
# for line in sys.stdin:
import csv

lines = []
with open('C:\\Users\\Gabri\\OneDrive\\Documenti\\Università\\BigData\\Progetti\\Progetto1\\Dataset\\test2.csv',
          newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        lines.append(row[0] + "," + row[1] + "," + row[2])
for line in lines:
    user_id, num, denom = line.strip().split(',')
    user_num_and_denom_list.append((user_id, int(num), int(denom)))

########################################################################
# Ordiniamo il dizionario in base al numero di recensioni per prodotto #
########################################################################
for user, num, denom in user_num_and_denom_list:
    if(user not in user_to_count_infos):
        user_to_count_infos[user] = (0,0)
    if(num != 0 and denom != 0):
        user_to_count_infos[user] = (user_to_count_infos[user][0] + (num/denom), user_to_count_infos[user][1] + 1)
    else:
        user_to_count_infos[user] = (user_to_count_infos[user][0], user_to_count_infos[user][1] + 1)

for user in user_to_count_infos:
    result[user] = user_to_count_infos[user][0]/user_to_count_infos[user][1]

print(result)