from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("Bronze").getOrCreate()
input_path = sys.argv[1]
repartition_num = int(sys.argv[2])
output_path = sys.argv[3]

df = spark.read.csv(input_path, header=False).limit(100000)

df.show(truncate=False)

repartition_df = df.repartition(repartition_num)
repartition_df.write.mode("overwrite").csv(output_path)
spark.stop()
