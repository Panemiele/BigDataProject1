#!/usr/bin/python
"""reducer.py"""

import sys
import collections
import itertools

########################
# VARIABLE DEFINITIONS #
########################
product_reviews_per_year = {}
first_10_prods = {}
words_count_in_review = {}
first_10_prods_in_year = {}
result = {}

#############
# FUNCTIONS #
#############
def addItemInProductReviewsPerYear(year, prod_id, text):
    if year in product_reviews_per_year:
        if prod_id in product_reviews_per_year[year]:
            product_reviews_per_year[year][prod_id].append(text)
        else:
            product_reviews_per_year[year][prod_id] = [text]
    else:
        product_reviews_per_year[year] = {prod_id: [text]}

def addItemInWordsCountInReview(text):
    if text in words_count_in_review:
        words_count_in_review[text] += 1
    else:
        words_count_in_review[text] = 1

def addItemInResult(year, prod_id, most_common_words):
    if year in result:
        if prod_id in result[year]:
            result[year][prod_id].append(most_common_words)
        else:
            result[year][prod_id] = [most_common_words]
    else:
        result[year] = {prod_id: [most_common_words]}

#######################################
# Leggi le righe dallo standard input #
#######################################
for line in sys.stdin:
    year_prod_id, text = line.strip().split('#_#_')
    year, prod_id = year_prod_id.strip().split('___')
    addItemInProductReviewsPerYear(year, prod_id, text)

########################################################################
# Ordiniamo il dizionario in base al numero di recensioni per prodotto #
########################################################################
for year, product_reviews in product_reviews_per_year.items():
    # Seleziona i primi 10 elementi dell'OrderedDict
    sorted_prods_reviews = collections.OrderedDict(
        sorted(product_reviews.items(), key=lambda x: len(x[1]), reverse=True))
    first_10_prods_reviews = list(sorted_prods_reviews.items())[:10]
    first_10_prods_in_year[year] = first_10_prods_reviews[0]

    #############################################################################################
    # Selezioniamo le 5 parole con almeno 4 caratteri più frequentemente usate nelle recensioni #
    #############################################################################################
    for prod, reviews in first_10_prods_reviews:
        words_counter = {}
        for review in reviews:
            words = review.strip().split()
            for w in words:
                if(len(w) >= 4):
                    if w in words_counter:
                        words_counter[w] += 1
                    else:
                        words_counter[w] = 1
        # Abbiamo la conta di ciascuna parola, bisogna selezionare le 5 più ricorrenti
        sorted_words_counter = collections.OrderedDict(sorted(words_counter.items(), key=lambda x: x[1], reverse=True))
        addItemInResult(year, prod, list(itertools.islice(sorted_words_counter.items(), 5)))

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