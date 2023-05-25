#!/usr/bin/python
"""spark application"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, row_number, count, desc, explode, split, length, asc

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path

start_time = time.time()
# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()

dataset = spark.read.csv(input_filepath).cache()
datasetDF = dataset\
    .toDF("Id", "ProductId", "UserId", "HelpfulnessNumerator", "HelpfulnessDenominator", "Score", "ReviewTime", "Text")


windowSpec = Window.partitionBy("ReviewTime").orderBy(desc("conta"))
typedDF = datasetDF.withColumn("ReviewTime", from_unixtime(col("ReviewTime"), "yyyy"))
aggregatedDF = typedDF\
    .select("ProductId", "ReviewTime")\
    .groupby("ReviewTime", "ProductId")\
    .agg(count("ProductId").alias("conta"))\
    .withColumn("rowNumber", row_number().over(windowSpec).cast(IntegerType()))
topTenProductsDF = aggregatedDF\
    .select("ProductId", "ReviewTime", "conta", "rowNumber")\
    .where(aggregatedDF["rowNumber"] <= 10)\
    .sort("ReviewTime", desc("rowNumber"))

joinDF = topTenProductsDF\
    .join(typedDF, (topTenProductsDF["ReviewTime"] == typedDF["ReviewTime"]) & (topTenProductsDF["ProductId"] == typedDF["ProductId"]))
selectJoinDF = joinDF.select(topTenProductsDF["ReviewTime"], topTenProductsDF["ProductId"], "Text")

windowSpec = Window.partitionBy("ReviewTime", "ProductId").orderBy(desc("conta"))
singleWordsDF = selectJoinDF.select("ReviewTime", "ProductId", "Text")\
    .withColumn("word", explode(split(selectJoinDF["Text"], ' ')))
semiResultDF = singleWordsDF.where(length("word") >= 4)\
    .groupby("ProductId", "ReviewTime", "word")\
    .agg(count("word").alias("conta"))\
    .withColumn("rowNumber", row_number().over(windowSpec).cast(IntegerType()))

resultDF = semiResultDF.select("ReviewTime", "ProductId", "word", "conta")\
    .where(semiResultDF["rowNumber"] <= 5)\
    .sort(asc("ReviewTime"), "ProductId")

resultDF.show()
end_time = time.time()
print("Total execution time: {} seconds".format(end_time - start_time))
