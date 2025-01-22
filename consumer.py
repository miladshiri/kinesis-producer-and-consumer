# This script should be run on a Databricks notebook

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col


# Define schema for incoming data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Set Kinesis stream parameters
kinesis_stream_name = "test_stream_for_purchases"
region = "eu-north-1"
kinesis_endpoint = f"https://kinesis.{region}.amazonaws.com"

# Read stream from Kinesis
raw_stream = spark.readStream.format("kinesis") \
    .option("streamName", kinesis_stream_name) \
    .option("region", region) \
    .option("initialPosition", "LATEST") \
    .option("awsAccessKey", 'copy_key_here') \
    .option("awsSecretKey", 'copy_key_here') \
    .load()

# Parse JSON data
parsed_stream = raw_stream.selectExpr("CAST(data AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")


display(parsed_stream)

# Write to a Delta table
query = parsed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/purchases") \
    .toTable("purchases")

query.awaitTermination()