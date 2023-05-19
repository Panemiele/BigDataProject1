#!/usr/bin/python
"""spark application"""

import argparse
from pyspark.sql import SparkSession

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

