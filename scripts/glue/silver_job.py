import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_bucket", "proc_date", "proc_hour"])
bucket = args["s3_bucket"]
proc_date = args["proc_date"]
proc_hour = args["proc_hour"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

bronze_path = f"s3://{bucket}/bronze/dt={proc_date}/hour={proc_hour}/"
print(f"Reading from: {bronze_path}")

# Read CSV
df = (
    spark.read
    .option("header", True)
    .csv(bronze_path)
)

print("Raw row count:", df.count())
print("Raw schema:")
df.printSchema()

# Cast and clean
df2 = (
    df.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("temp_c", col("temp_c").cast(DoubleType()))
      .withColumn("humidity_pc", col("humidity_pc").cast(DoubleType()))
      .withColumn("vibration_g", col("vibration_g").cast(DoubleType()))
      .dropna(subset=["event_time", "device_id"])
)


df2.select("event_time", "device_id").show(5, truncate=False)

print("Cleaned row count:", df2.count())

# Add partitions
df3 = (
    df2.withColumn("year", year(col("event_time")))
       .withColumn("month", month(col("event_time")))
       .withColumn("day", dayofmonth(col("event_time")))
       .withColumn("hour", hour(col("event_time")))
)

print("Final row count (with partitions):", df3.count())

silver_path = f"s3://{bucket}/silver/"
print(f"Writing to: {silver_path}")

(
    df3.repartition("year", "month", "day", "hour", "device_id")
       .write.mode("append")
       .partitionBy("year", "month", "day", "hour", "device_id")
       .parquet(silver_path)
)

print("Write completed")
