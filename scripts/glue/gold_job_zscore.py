import sys, json, io, boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, avg, stddev, abs as sql_abs, count, coalesce, lit
)

# -----------------------------
# Args
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_bucket"])
bucket = args["s3_bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------
# Read all Silver data
# -----------------------------
silver_base = f"s3://{bucket}/silver/"
df = (
    spark.read
         .option("basePath", silver_base)
         .parquet(f"{silver_base}/year=*/month=*/day=*/hour=*/")
)

print("Silver schema:")
df.printSchema()
print("Silver row count:", df.count())

# -----------------------------
# Z-score anomaly detection
# -----------------------------
w = Window.partitionBy("device_id")
stats = (df
    .withColumn("avg_t", avg("temp_c").over(w))
    .withColumn("std_t", stddev("temp_c").over(w))
    .withColumn("avg_h", avg("humidity_pc").over(w))
    .withColumn("std_h", stddev("humidity_pc").over(w))
    .withColumn("avg_v", avg("vibration_g").over(w))
    .withColumn("std_v", stddev("vibration_g").over(w))
)

def z(colname, mean, std):
    return (col(colname) - col(mean)) / col(std)

zs = (stats
    .withColumn("z_temp", z("temp_c", "avg_t", "std_t"))
    .withColumn("z_hum",  z("humidity_pc", "avg_h", "std_h"))
    .withColumn("z_vib",  z("vibration_g", "avg_v", "std_v"))
)

# Replace null/NaN z-scores with 0
safe = (zs
    .withColumn("z_temp", coalesce(col("z_temp"), lit(0.0)))
    .withColumn("z_hum",  coalesce(col("z_hum"),  lit(0.0)))
    .withColumn("z_vib",  coalesce(col("z_vib"),  lit(0.0)))
)

# Flag anomalies: any |z| > 3
anomalies = safe.filter(
    (sql_abs(col("z_temp")) > 3) |
    (sql_abs(col("z_hum")) > 3) |
    (sql_abs(col("z_vib")) > 3)
)

print("Anomalies count:", anomalies.count())

# -----------------------------
# Write Gold anomalies to S3
# -----------------------------
gold_path = f"s3://{bucket}/gold/"
(anomalies
 .write
 .mode("append")
 .partitionBy("year", "month", "day", "hour", "device_id")
 .parquet(gold_path))

print(f"Wrote anomalies to {gold_path}")

# -----------------------------
# Write summary JSON (compact)
# -----------------------------
summary = (anomalies
    .groupBy("year", "month", "day", "hour")
    .agg(count(lit(1)).alias("anomaly_count"))
)

rows = summary.collect()
summary_key = "gold_summary/summary.json"

s3 = boto3.client("s3")
buf = io.BytesIO((json.dumps([r.asDict() for r in rows]) + "\n").encode("utf-8"))
s3.upload_fileobj(buf, bucket, summary_key)

print(f"Wrote summary to s3://{bucket}/{summary_key}")
