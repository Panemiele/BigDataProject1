"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, collect_set, array_contains
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
    .withColumn("Score", col("Score").cast(IntegerType()))

filterScoreDF = typedDF\
    .filter(typedDF["Score"] >= 4)\

setProductsDF= filterScoreDF\
     .groupBy("UserId").agg(collect_set("ProductId").alias("AllProducts"))

usersDF =setProductsDF\
     .filter(count(col("AllProducts")) >= 3)

# Join per creare i gruppi (sfrutto inner join sulla condizione)
groupsDF = usersDF.alias("u").join(datasetDF.alias("p"), (usersDF["user_id"] != datasetDF["UserId"]) & array_contains(usersDF["AllProducts"], datasetDF["ProductId"]), "inner") \
    .groupBy("u.UserId") \
    .agg(collect_set("p.UserId").alias("users"), collect_set("p.ProductId").alias("common_products")) \
    .orderBy("u.UserId")

# Filtro gruppi con almeno 2 utenti
resultDF = groupsDF.filter("size(users) > 1")

# Show the result
result_DF.show()
