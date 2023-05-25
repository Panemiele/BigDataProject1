"""spark application"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, array_intersect, size, concat, array, first
from pyspark.sql.types import IntegerType

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = "hdfs:///input/datasetDoubled.csv"

start_time = time.time()
# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

dataset = spark.read.csv(input_filepath).cache()

datasetDF = dataset.toDF("Id", "ProductId", "UserId", "HelpfulnessNumerator", "HelpfulnessDenominator", "Score",
                         "ReviewTime", "Text")

typedDF = datasetDF \
    .withColumn("Score", col("Score").cast(IntegerType()))

filterScoreDF = typedDF \
    .where(typedDF["Score"] >= 4)

setProductDF = filterScoreDF.groupBy("UserId").agg(collect_set("ProductId").alias("AllProducts"))
filterUserProdsDF = setProductDF.where(size("AllProducts") >= 3).cache()
sortedUsers = filterUserProdsDF.sort("UserId").cache()

usersPairDF = sortedUsers.alias("a").join(sortedUsers.alias("b"), size(array_intersect("a.AllProducts", "b.AllProducts")) >= 3).where("a.UserId != b.UserId")
groupsDF = usersPairDF.groupBy("a.AllProducts").agg(collect_set("b.UserId").alias("users")).cache()
groupUsersDF = groupsDF.withColumn("groupUsers", concat(array("UserId"), "users")).groupBy("AllProducts")

result = groupUsersDF.select("groupUsers", "AllPRoducts").groupBy("AllProducts").agg(collect_set("groupUsers"))

# Show the result
result.show()
end_time = time.time()
print("Total execution time: {} seconds".format(end_time - start_time))
