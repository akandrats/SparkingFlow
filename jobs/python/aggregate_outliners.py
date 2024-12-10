from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, lit, row_number
import sys
import os

spark = SparkSession.builder.appName("Gold-outliners").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

df = spark.read.csv(input_path, header=True, inferSchema=True)

filtered_df = df.filter(col("HRESULT").contains("0x800f080d"))
filtered_df.show(truncate=False)
filtered_df.repartition(1).write.mode("overwrite").csv(output_path, header=True)
spark.stop()
