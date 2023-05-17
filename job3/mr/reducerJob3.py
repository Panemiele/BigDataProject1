#!/usr/bin/python
"""reducerJob3.py"""

import sys
import collections
import itertools

########################
# VARIABLE DEFINITIONS #
########################
user_product_dict = {}
user_to_group = {}                    # Dizionario che associa ad ogni utente il gruppo di appartenenza
group_to_similar_users_items = {}     # Dizionario che associa ad ogni gruppo una coppia (setUtenti, setProdotti)
last_used_group = 0

#############
# FUNCTIONS #
#############
def addItemInUserProductDict(user_id, prod_id, score):
    if user_id not in user_product_dict:
        user_product_dict[user_id] = []
    if(score >= 4):
        user_product_dict[user_id].append((prod_id, score))

def removeItemsWithLessThanNOccurrencies(dictionary, n):
    for key in dictionary.keys():
        if(len(dictionary[key]) < n):
            del dictionary[key]

def chooseGroupByItemLists(user_to_list1, user_to_list2):
    k1 = user_to_list1.key
    k2 = user_to_list2.key
    shared_products = compareItemsBetweenUsers(user_to_list1.value, user_to_list2.value)
    if(len(shared_products) >= 3):
        if(k1 in user_to_group):
            if(k2 not in user_to_group):
                g1 = user_to_group[k1]
                user_to_group[k2] = g1
                group_to_similar_users_items[g1] =\
                    (group_to_similar_users_items[g1][0].add(k2), group_to_similar_users_items[g1][1].add(shared_products))
        else:
            if(k2 in user_to_group):
                g2 = user_to_group[k2]
                user_to_group[k1] = g2
                group_to_similar_users_items[g2] = \
                    (group_to_similar_users_items[g2][0].add(k1),
                     group_to_similar_users_items[g2][1].add(shared_products))
            else:
                group_to_similar_users_items[last_used_group] = [].append(k1, k2)
                last_used_group += 1

def compareItemsBetweenUsers(itemList1, itemList2):
    shared_products = []
    for i1 in itemList1:
        for i2 in itemList2:
            if i1 == i2:
                shared_products.append(i1)
    return shared_products

#######################################
# Leggi le righe dallo standard input #
#######################################
for line in sys.stdin:
    user_id, prod_id, score = line.strip().split(',')
    addItemInUserProductDict(user_id, prod_id, score)

########################################################################
# Ordiniamo il dizionario in base al numero di recensioni per prodotto #
########################################################################

removeItemsWithLessThanNOccurrencies(user_product_dict, 3)
for user_to_list1 in user_product_dict:
    for user_to_list2 in user_product_dict:
        if user_to_list1.key == user_to_list2.key: continue
        chooseGroupByItemLists(user_to_list1, user_to_list2)





















##################################
# Stampa il risultato formattato #
##################################
for year, prod_id_review in result.items():
    is_year_printed = False
    for prod_id, reviews in prod_id_review.items():
        for review in reviews:
            if(is_year_printed == False):
                print("%i\t%s\t%s" % (int(year), prod_id, review))
                is_year_printed = True
            else:
                print("\t%s\t%s" % (prod_id, review))