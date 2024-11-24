from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, lag, concat_ws, lit, row_number
from pyspark.sql.window import Window
import sys

spark = SparkSession.builder.appName("Silver-session").getOrCreate()

input_path = sys.argv[1]
repartition_num = int(sys.argv[2])
output_path = sys.argv[3]

df = spark.read.text(input_path).withColumnRenamed("value", "log_line")

timestamp_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
initialized_by_pattern = r"initialized by client (\S+)"

parsed_df = df.withColumn("StartTime", regexp_extract(col("log_line"), timestamp_pattern, 1)) \
              .withColumn("EndTime", regexp_extract(col("log_line"), timestamp_pattern, 1)) \
              .withColumn("InitializedBy", regexp_extract(col("log_line"), initialized_by_pattern, 1)) \
              .withColumn("Id", lit(None))  # Placeholder for Id

window_spec = Window.orderBy("StartTime")
parsed_df = parsed_df.withColumn("Id", row_number().over(window_spec))

final_df = parsed_df.filter(col("InitializedBy") != "") \
                    .select("Id", "StartTime", "EndTime", "InitializedBy")

repartition_df = final_df.repartition(repartition_num)
repartition_df.write.mode("overwrite").csv(output_path, header=True)

spark.stop()