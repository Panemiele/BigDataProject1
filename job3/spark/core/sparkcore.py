#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession
import re
import time

regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job3 Spark") \
    .config("spark.executor.instances", 15) \
    .getOrCreate()

# read the input file and obtain an RDD with a record for each line
rdd = spark.sparkContext.textFile("Input").cache()

# remove csv header
removedHeaderRDD = rdd.filter(f=lambda word: not word.startswith("Id") and not word.endswith("Text"))

# Filtra le righe con uno score >= 4
filteredScoreRDD = removedHeaderRDD.filter(f=lambda line: int(re.split(regex, line)[6]) >= 4)

# Crea la coppia (userId, productId) per ogni riga
user2ProductsRDD = filteredScoreRDD.map(f=lambda line: (re.split(regex, line)[2], re.split(regex, line)[1])

# Raggruppa i prodotti per utente
userProductsRDD = user2ProductsRDD.groupByKey().mapValues(set)

# Filtra gli utenti con almeno 3 prodotti
activeUsersRDD = userProductsRDD.filter(lambda line: len(line[1]) >= 3)

# Creo gruppi di utenti con prodotti in comune
groupsRDD = activeUsersRDD.flatMap(lambda couple: [(user, couple[0]) for user in couple[1]]) \
    .groupByKey() \
    .flatMap(lambda couple: [(tuple(sorted(couple[1])), couple[0])]) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda couple: len(set(couple[1])) >= 3) \
    .mapValues(lambda users: (list(set(users)), list(set(userProducts[user] for user in users)))).values()

start_time = time.time()
groupsRDD.collect()
end_time = time.time()
print("Total execution time: {} seconds".format(end_time - start_time))
print("ok")

groupsRDD.saveAsTextFile("Output")