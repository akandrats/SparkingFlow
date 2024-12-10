from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, lit, row_number
from pyspark.sql.window import Window
import sys

spark = SparkSession.builder.appName("Silver-outliners").getOrCreate()

input_path = sys.argv[1]
repartition_num = int(sys.argv[2])
output_path = sys.argv[3]

df = spark.read.text(input_path).withColumnRenamed("value", "log_line")

hresult_pattern = r"HRESULT\s*=\s*(0x[0-9a-fA-F]+)"

parsed_df = df.withColumn("Timestamp", regexp_extract(col("log_line"), r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", 1)) \
              .withColumn("HRESULT", regexp_extract(col("log_line"), hresult_pattern, 1)) \
              .withColumn("Id", lit(None))  # Placeholder for Id

# Assign unique IDs
window_spec = Window.orderBy("Timestamp")
parsed_df = parsed_df.withColumn("Id", row_number().over(window_spec))

# Filter rows where HRESULT is not null and select relevant columns
final_df = parsed_df.filter(col("HRESULT") != "") \
                    .select("Id", "Timestamp", "HRESULT")

repartition_df = final_df.repartition(repartition_num)
repartition_df.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
