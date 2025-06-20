"""
Earthquake and Fire Data Consumer using Spark Structured Streaming and Kafka

This script consumes earthquake and fire data from Kafka topics, processes them using Spark Structured Streaming,
and saves the results to HBase. It implements deduplication and categorization of events.

The consumer:
1. Reads from two Kafka topics: 'earthquakes' and 'fires'
2. Processes each stream separately based on its type
3. Deduplicates events using stateful processing
4. Saves categorized events to HBase

Key features:
- Real-time processing using Spark Structured Streaming
- Stateful deduplication of events
- Separate processing for each event type
- HBase storage with timestamp-based partitioning
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

# HBase configuration
HBASE_HOST = "hadoop-master"
HBASE_PORT = "2181"
HBASE_ZOOKEEPER_QUORUM = f"{HBASE_HOST}:{HBASE_PORT}"

# Define checkpoint directory
checkpoint_dir = "/tmp/spark_checkpoints"
logger.info(f"Setting up checkpoint directory: {checkpoint_dir}")

# Create Spark session with Kafka connector
logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("EarthquakeStream") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.hadoop.hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM) \
    .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181") \
    .getOrCreate()

# Enable checkpointing for fault tolerance
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
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse the JSON data
    parsed_stream = stream.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")
    
    # Add debug logging
    logger.info(f"Schema for {topic}: {parsed_stream.schema}")
    
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

# Function to write to HBase
def write_to_hbase(df, epoch_id, table_name):
    # Convert DataFrame to HBase format
    hbase_data = df.select(
        col("id").alias("rowkey"),
        struct(
            lit("info").alias("cf"),
            col("*")
        ).alias("data")
    )
    
    # Write to HBase using foreachBatch
    hbase_data.write \
        .format("org.apache.hadoop.hbase.spark") \
        .option("hbase.table", table_name) \
        .option("hbase.columns.mapping", "rowkey:key,data:info") \
        .mode("append") \
        .save()

# Write earthquake events to HBase
logger.info("Setting up earthquake events write stream to HBase...")
earthquake_query = earthquake_events.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_hbase(df, epoch_id, "earthquake_events")) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .queryName("earthquake_stream") \
    .start()

# Write fire events to HBase
logger.info("Setting up fire events write stream to HBase...")
fire_query = fire_events.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_hbase(df, epoch_id, "fire_events")) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .queryName("fire_stream") \
    .start()

logger.info("Starting streaming queries...")

# Wait for both streaming queries to terminate
earthquake_query.awaitTermination()
fire_query.awaitTermination()