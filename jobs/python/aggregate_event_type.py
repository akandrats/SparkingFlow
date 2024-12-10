from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, lag, concat_ws, coalesce
import sys
import os

spark = SparkSession.builder.appName("Gold-event-type").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

df = spark.read.csv(input_path, header=True, inferSchema=True)

filtered_df = df.filter(
    (col("log_level") == "Info") &
    (col("timestamp") >= "2016-10-1") &
    (col("timestamp") <= "2016-10-30")
)

filtered_df.repartition(1).write.mode("overwrite").csv(output_path, header=True)
spark.stop()
