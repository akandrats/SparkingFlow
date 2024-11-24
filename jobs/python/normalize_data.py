from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, lag, concat_ws, coalesce
from pyspark.sql.window import Window
import sys

spark = SparkSession.builder.appName("Silver").getOrCreate()
input_path = sys.argv[1]
repartition_num = int(sys.argv[2])
output_path = sys.argv[3]

df = spark.read.csv(input_path, header=False).withColumnRenamed("_c1", "value")

timestamp_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
log_level_pattern = r"(Info|Error|Warning|Debug)\s+"
component_pattern = r"\s+(CBS|CSI)\s+"
message_pattern = r"(?:Info|Error|Warning|Debug)\s+\S+\s+(.*)"

parsed_df = df.select(
    regexp_extract(col("_c0"), timestamp_pattern, 1).alias("timestamp"),
    regexp_extract(col("value"), log_level_pattern, 1).alias("log_level"),
    regexp_extract(col("value"), component_pattern, 1).alias("component"),
    regexp_extract(col("value"), message_pattern, 1).alias("message")
)

parsed_df = parsed_df.withColumn(
    "is_continuation",
    when(col("message").rlike("^CSIPERF"), 1).otherwise(0)
)

window_spec = Window.orderBy("timestamp")
parsed_df = parsed_df.withColumn(
    "group",
    when(col("is_continuation") == 1, lag("timestamp", 1).over(window_spec)).otherwise(col("timestamp"))
)

parsed_df = parsed_df.withColumn(
    "message",
    when(
        col("is_continuation") == 1,
        lag("message", 1).over(window_spec) + " " + col("message")
    ).otherwise(col("message"))
)

parsed_df = parsed_df.filter(col("is_continuation") == 0)

repartition_df = parsed_df.repartition(repartition_num)
repartition_df.write.mode("overwrite").csv(output_path, header=True)
spark.stop()
