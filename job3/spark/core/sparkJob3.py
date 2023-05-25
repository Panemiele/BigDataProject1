#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession
import re
import time

def setCommonProductsPerCouple(user1, user2):
    u1prods = user1[1]
    u2prods = user2[1]
    if len(u1prods.intersection(u2prods)) < 3:
        return user1[0], ([], {})
    return user1[0], ({user2[0]}, u1prods.intersection(u2prods))

regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
# input_filepath, output_filepath = args.input_path, args.output_path
input_filepath = "hdfs:///input/datasetDoubled.csv"
output_filepath = "hdfs:///output/job3/spark/core"

start_time = time.time()

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job3 Spark") \
    .config("spark.executor.instances", 15) \
    .getOrCreate()

# read the input file and obtain an RDD with a record for each line
rdd = spark.sparkContext.textFile(input_filepath).cache()

# remove csv header
removedHeaderRDD = rdd.filter(f=lambda word: not word.startswith("Id") and not word.endswith("Text"))

# Filtra le righe con uno score >= 4
filteredScoreRDD = removedHeaderRDD.filter(f=lambda line: int(re.split(regex, line)[6]) >= 4)

# Crea la coppia (userId, productId) per ogni riga
user2ProductsRDD = filteredScoreRDD.map(f=lambda line: (re.split(regex, line)[2], re.split(regex, line)[1]))

# Raggruppa i prodotti per utente
userProductsRDD = user2ProductsRDD.groupByKey().mapValues(set)

# Filtra gli utenti con almeno 3 prodotti
activeUsersRDD = userProductsRDD.filter(lambda line: len(line[1]) >= 3).cache()

groupByRDD = activeUsersRDD\
    .groupBy(lambda x: frozenset.intersection(frozenset(x[1])))\
    .filter(lambda x: len(x[1]) >= 3 & len(x[0]) >= 2)

mappedValuesRDD = groupByRDD.map(lambda x: (set(x[0]), x[1])).mapValues(list).mapValues(lambda x: [y[0] for y in x])
invertedRDD = mappedValuesRDD.map(lambda x: (x[1], x[0]))
resultRDD = invertedRDD.sortBy(lambda x: x[0][0])


resultRDD.collect()
end_time = time.time()
print("Total execution time: {} seconds".format(end_time - start_time))
resultRDD.saveAsTextFile(output_filepath)
