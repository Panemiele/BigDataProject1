#!/usr/bin/python
"""spark job1 application"""

import argparse
from pyspark.sql import SparkSession
from datetime import datetime
import re
import time

def calcolaUtilita(num, denom):
    if int(denom) == 0:
        return 0, 0
    return float(num) / int(denom), 1

def calcolaApprezzamento(utilita, conta):
    if conta == 0:
        return 0
    return utilita/conta


# Espressione regolare usata per splittare la line in input
regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

start_time = time.time()

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job1 Spark") \
    .getOrCreate()

# read the input file and obtain an RDD with a record for each line
rdd = spark.sparkContext.textFile(input_filepath).cache()

# remove csv header
removedHeaderRDD = rdd.filter(f=lambda word: not word.startswith("Id") and not word.endswith("Text"))

# (Anno, (ProdId, (1, Text)))
parsedRDD = removedHeaderRDD.map(
    f=lambda line:
        (datetime.fromtimestamp(int(re.split(regex, line)[6])).strftime('%Y'),
         re.split(regex, line)[1],
         re.split(regex, line)[7])
)

# Calcolo del numero di recensioni per ogni prodotto in ogni anno
prodReviewsCountRDD = parsedRDD.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y).cache()

# Ordinamento dei prodotti per il numero di recensioni in ordine decrescente
sortedProdReviewsRDD = prodReviewsCountRDD.sortBy(lambda x: x[1], ascending=False)\
    .map(lambda x: (x[0][0], x[0][1]))

# Prendere i primi 10 prodotti per ogni anno
topProductsRDD = sortedProdReviewsRDD\
    .groupByKey()\
    .mapValues(list)\
    .map(lambda x: (x[0], x[1][:10]))\
    .flatMapValues(list)\
    .map(lambda x: (x, 1))\
    .cache()

# Estrazione delle parole dalle recensioni
yearProd2FilteredWordsRDD = parsedRDD\
    .map(lambda x: ((x[0], x[1]), list(filter(lambda word: len(word) >= 4, x[2].strip().split()))))\
    .flatMapValues(list)
yearProdAndWord2CountRDD = yearProd2FilteredWordsRDD\
    .map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda a, b: a + b)\
    .cache()
sortedYearProdWordsRDD = yearProdAndWord2CountRDD.sortBy(lambda x: x[1], ascending=False)
year2ProdSortedWords = sortedYearProdWordsRDD\
    .map(lambda x: (x[0][0], (x[0][1], x[1])))
top5WordsPerYearProduct = year2ProdSortedWords.groupByKey().mapValues(list) \
    .map(lambda x: (x[0], x[1][:5]))\
    .cache()

# Unione dei conteggi delle parole con i prodotti
joinRDD = topProductsRDD.join(top5WordsPerYearProduct)

# Raggruppamento delle parole per prodotto e anno
groupedWordsRDD = joinRDD.map(lambda x: ((x[0][0], x[0][1]), x[1][1])).cache()
result = groupedWordsRDD.sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])

result.collect()
end_time = time.time()
print("Total execution time: {} seconds".format(end_time - start_time))

# write all <year, list of (word, occurrence)> pairs in file
result.saveAsTextFile(output_filepath)
