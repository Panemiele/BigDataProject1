#!/usr/bin/python
"""reducerJob2.py"""

import sys

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
    # Inizializza il dizionario per il particolare utente
    if (user not in user_to_count_infos):
        user_to_count_infos[user] = (0, 0)
    if(denom != 0):
        user_to_count_infos[user] = (user_to_count_infos[user][0] + (num / denom), user_to_count_infos[user][1] + 1)

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
    if(user_to_count_infos[user][1] != 0):
        result[user] = user_to_count_infos[user][0]/user_to_count_infos[user][1]
    else:
        result[user] = 0

######################
# Risultato ordinato #
######################
sorted_result_items = sorted(result.items(), key=lambda x: x[1], reverse=True)
for item in sorted_result_items:
    print(item)