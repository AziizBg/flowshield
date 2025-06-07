"""
Earthquake and Fire Data Consumer using Spark Structured Streaming and Kafka

This script consumes earthquake and fire data from Kafka topics, processes them using Spark Structured Streaming,
and saves the results to HDFS. It implements deduplication and categorization of events.

The consumer:
1. Reads from two Kafka topics: 'earthquakes' and 'fires'
2. Processes each stream separately based on its type
3. Deduplicates events using stateful processing
4. Saves categorized events to HDFS

Key features:
- Real-time processing using Spark Structured Streaming
- Stateful deduplication of events
- Separate processing for each event type
- HDFS storage with timestamp-based partitioning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import json
import datetime

# Create Spark session with Kafka connector
spark = SparkSession.builder \
    .appName("EarthquakeStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Enable checkpointing for fault tolerance
spark.sparkContext.setCheckpointDir("hdfs:///checkpoints/earthquake_stream")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Kafka broker address
EARTHQUAKE_TOPIC = 'earthquakes'            # Topic for earthquake events
FIRE_TOPIC = 'fires'                        # Topic for fire events

# Define schemas for parsing JSON
earthquake_schema = StructType([
    StructField("id", StringType()),
    StructField("time", TimestampType()),
    StructField("magnitude", DoubleType()),
    StructField("place", StringType()),
    StructField("url", StringType()),
    StructField("status", StringType()),
    StructField("magType", StringType())
])

fire_schema = StructType([
    StructField("id", StringType()),
    StructField("time", TimestampType()),
    StructField("frp", DoubleType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

def get_fire_severity(frp):
    """
    Determines the severity level of a fire based on its Fire Radiative Power (FRP).
    
    Args:
        frp (float): Fire Radiative Power value
        
    Returns:
        str: Severity level ('Low', 'Moderate', or 'High')
    """
    if frp < 5:
        return "Low"
    elif frp < 15:
        return "Moderate"
    else:
        return "High"

# Create Kafka streams for both topics
def create_kafka_stream(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

# Create streams
earthquake_stream = create_kafka_stream(EARTHQUAKE_TOPIC, earthquake_schema)
fire_stream = create_kafka_stream(FIRE_TOPIC, fire_schema)

# Process earthquake stream
earthquake_events = earthquake_stream \
    .withColumn("type", lit("earthquake")) \
    .withColumn("city", expr("split(place, ',')[0]")) \
    .withColumn("country", expr("split(place, ',')[1]")) \
    .dropDuplicates(["id"])

# Process fire stream
fire_events = fire_stream \
    .withColumn("type", lit("fire")) \
    .withColumn("severity", expr("CASE WHEN frp < 5 THEN 'Low' WHEN frp < 15 THEN 'Moderate' ELSE 'High' END")) \
    .dropDuplicates(["id"])

# Combine the processed streams
all_events = earthquake_events.union(fire_events)

# Write the stream to HDFS
query = all_events.writeStream \
    .format("json") \
    .option("path", "hdfs:///events") \
    .option("checkpointLocation", "hdfs:///checkpoints/earthquake_stream") \
    .partitionBy("type") \
    .trigger(processingTime='10 seconds') \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()