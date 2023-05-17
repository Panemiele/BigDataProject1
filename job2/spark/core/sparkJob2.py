#!/usr/bin/python
"""spark job2 application"""

import argparse
from pyspark.sql import SparkSession
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

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job2 Spark") \
    .getOrCreate()

# read the input file and obtain an RDD with a record for each line
rdd = spark.sparkContext.textFile(input_filepath).cache()

# remove csv header
removedHeaderRDD = rdd.filter(f=lambda word: not word.startswith("Id") and not word.endswith("Text"))

user2NumDenomRDD = removedHeaderRDD.map(
    f=lambda line: (re.split(regex, line)[2], re.split(regex, line)[3], re.split(regex, line)[4]))
user2UtilityRDD = user2NumDenomRDD.map(f=lambda info: (info[0], calcolaUtilita(info[1], info[2])))
user2SumUtilityRDD = user2UtilityRDD.reduceByKey(func=lambda x, y: (x[0] + y[0], x[1] + y[1]))
user2apprRDD = user2SumUtilityRDD.map(f=lambda item: (item[0], calcolaApprezzamento(item[1][0], item[1][1])))
result = user2apprRDD.sortBy(lambda item: item[1], ascending=False)

start_time = time.time()
result.collect()
end_time = time.time()
print("Total execution time: {} seconds".format(end_time - start_time))
print("ok")

# write all <year, list of (word, occurrence)> pairs in file
result.saveAsTextFile(output_filepath)
