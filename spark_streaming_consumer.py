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
from pyspark.sql.functions import from_json, col, expr, struct, lit, to_json, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import json
import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# HDFS configuration
HDFS_HOST = "hadoop-master"
HDFS_PORT = "9000"
HDFS_BASE_PATH = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"

# Create Spark session with Kafka connector
logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("EarthquakeStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Enable checkpointing for fault tolerance
checkpoint_dir = f"{HDFS_BASE_PATH}/checkpoints/earthquake_stream"
logger.info(f"Setting up checkpoint directory: {checkpoint_dir}")
spark.sparkContext.setCheckpointDir(checkpoint_dir)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Kafka broker address
EARTHQUAKE_TOPIC = 'earthquakes'            # Topic for earthquake events
FIRE_TOPIC = 'fires'                        # Topic for fire events

logger.info(f"Kafka configuration: bootstrap_servers={KAFKA_BOOTSTRAP_SERVERS}, topics=[{EARTHQUAKE_TOPIC}, {FIRE_TOPIC}]")

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

# Create Kafka streams for both topics
def create_kafka_stream(topic, schema):
    logger.info(f"Creating Kafka stream for topic: {topic}")
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Log the raw data count
    logger.info(f"Raw data count for {topic}: {stream.count()}")
    
    # Parse the JSON data
    parsed_stream = stream.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")
    
    # Log the parsed data count
    logger.info(f"Parsed data count for {topic}: {parsed_stream.count()}")
    
    return parsed_stream

# Create streams
logger.info("Initializing Kafka streams...")
earthquake_stream = create_kafka_stream(EARTHQUAKE_TOPIC, earthquake_schema)
fire_stream = create_kafka_stream(FIRE_TOPIC, fire_schema)

# Process earthquake stream
logger.info("Processing earthquake stream...")
earthquake_events = earthquake_stream \
    .withColumn("type", lit("earthquake")) \
    .withColumn("city", expr("split(place, ',')[0]")) \
    .withColumn("country", expr("split(place, ',')[1]")) \
    .withColumn("severity", expr("CASE WHEN magnitude < 4.0 THEN 'Low' WHEN magnitude < 6.0 THEN 'Moderate' ELSE 'High' END")) \
    .withColumn("latitude", lit(None).cast(DoubleType())) \
    .withColumn("longitude", lit(None).cast(DoubleType())) \
    .dropDuplicates(["id"])

# Log earthquake event count
logger.info(f"Processed earthquake events count: {earthquake_events.count()}")

# Process fire stream
logger.info("Processing fire stream...")
fire_events = fire_stream \
    .withColumn("type", lit("fire")) \
    .withColumn("severity", expr("CASE WHEN frp < 5 THEN 'Low' WHEN frp < 15 THEN 'Moderate' ELSE 'High' END")) \
    .withColumn("magnitude", lit(None).cast(DoubleType())) \
    .withColumn("place", lit(None).cast(StringType())) \
    .withColumn("url", lit(None).cast(StringType())) \
    .withColumn("status", lit(None).cast(StringType())) \
    .withColumn("magType", lit(None).cast(StringType())) \
    .dropDuplicates(["id"])

# Log fire event count
logger.info(f"Processed fire events count: {fire_events.count()}")

# Write earthquake events to HDFS
earthquake_output_path = f"{HDFS_BASE_PATH}/events/earthquake"
earthquake_checkpoint_path = f"{HDFS_BASE_PATH}/checkpoints/earthquake_stream/earthquake"
logger.info(f"Setting up earthquake events write stream to: {earthquake_output_path}")

# Add a query name for better monitoring
earthquake_query = earthquake_events.writeStream \
    .format("json") \
    .option("path", earthquake_output_path) \
    .option("checkpointLocation", earthquake_checkpoint_path) \
    .trigger(processingTime='10 seconds') \
    .queryName("earthquake_stream") \
    .start()

# Write fire events to HDFS
fire_output_path = f"{HDFS_BASE_PATH}/events/fire"
fire_checkpoint_path = f"{HDFS_BASE_PATH}/checkpoints/earthquake_stream/fire"
logger.info(f"Setting up fire events write stream to: {fire_output_path}")

# Add a query name for better monitoring
fire_query = fire_events.writeStream \
    .format("json") \
    .option("path", fire_output_path) \
    .option("checkpointLocation", fire_checkpoint_path) \
    .trigger(processingTime='10 seconds') \
    .queryName("fire_stream") \
    .start()

logger.info("Starting streaming queries...")

# Wait for both streaming queries to terminate
earthquake_query.awaitTermination()
fire_query.awaitTermination()