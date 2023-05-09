#!/usr/bin/python
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
def addItemInUserToCountInfos(user, num, denom):
    if (user not in user_to_count_infos):
        user_to_count_infos[user] = (0, 0)
    if (num != 0 and denom != 0):
        user_to_count_infos[user] = (user_to_count_infos[user][0] + (num / denom), user_to_count_infos[user][1] + 1)
    else:
        user_to_count_infos[user] = (user_to_count_infos[user][0], user_to_count_infos[user][1] + 1)

#######################################
# Leggi le righe dallo standard input #
#######################################
for line in sys.stdin:
    user_id, num, denom = line.strip().split(',')
    user_num_and_denom_list.append((user_id, int(num), int(denom)))

############################################################
# Per ciascuno user, calcolare la media dell'apprezzamento #
############################################################
for user, num, denom in user_num_and_denom_list:
    addItemInUserToCountInfos(user, num, denom)
for user in user_to_count_infos:
    result[user] = user_to_count_infos[user][0]/user_to_count_infos[user][1]

######################
# Risultato ordinato #
######################
print(sorted(result.items(), key=lambda x: x[1], reverse=True))