#!/usr/bin/python
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import avg
from pyspark.sql.types import IntegerType

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()

dataset = spark.read.csv(input_filepath).cache()
datasetDF = dataset.toDF("Id", "ProductId", "UserId", "HelpfulnessNumerator", "HelpfulnessDenominator", "Score", "ReviewTime", "Text")

typedDF = datasetDF\
    .withColumn("HelpfulnessNumerator", col("HelpfulnessNumerator").cast(IntegerType()))\
    .withColumn("HelpfulnessDenominator", col("HelpfulnessDenominator").cast(IntegerType()))
infoDF = typedDF\
    .select("UserId", "HelpfulnessNumerator", "HelpfulnessDenominator")\
    .where(typedDF["HelpfulnessDenominator"] != 0)
user2UtilityDF = infoDF.groupby("UserId").agg(avg((col("HelpfulnessNumerator") * 1.0) / col("HelpfulnessDenominator")).alias("Apprezzamento"))
result = user2UtilityDF.sort(user2UtilityDF[1].desc)

result.show()