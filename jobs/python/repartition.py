from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
import sys

spark = SparkSession.builder.appName("Bronze").getOrCreate()
input_path = sys.argv[1]
repartition_num = int(sys.argv[2])
output_path = sys.argv[3]

df = spark.read.csv(input_path, header=False)

df.show(truncate=False)

repartition_df = df.repartition(repartition_num)
repartition_df.write.mode("overwrite").csv(output_path)
#repartition_df.write(output_path) # dokumentacja spark write csv, overwrite parameter (?) - output_path

spark.stop()

# Regular expression patterns for parsing
timestamp_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
log_level_pattern = r"\s+(Info|Error|Warning|Debug)\s+"
component_pattern = r"CSI\s+(\d+)"
message_pattern = r"(.*)$"

# Extract fields using regular expressions
parsed_df = df.select(
    regexp_extract("value", timestamp_pattern, 1).alias("timestamp"),
    regexp_extract("value", log_level_pattern, 1).alias("log_level"),
    regexp_extract("value", component_pattern, 1).alias("component_id"),
    regexp_extract("value", message_pattern, 1).alias("message")
)

# Show the parsed result
parsed_df.show(truncate=False)
