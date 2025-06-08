"""
Earthquake and Fire Data Consumer - Alternative Storage Version

This version avoids HBase connector issues by providing multiple storage options:
1. JSON files (for debugging)
2. Parquet files (for analytics)
3. Console output (for testing)
4. HBase REST API (when available)

Run with:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer_alternative.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, struct, lit, to_json, count, 
    current_timestamp, when, window, sum as spark_sum, avg, max as spark_max
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import json
import datetime
import logging
import os
import requests
import base64
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamingConfig:
    """Configuration class for the streaming application"""
    
    def __init__(self):
        # Kafka configuration
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.EARTHQUAKE_TOPIC = os.getenv('EARTHQUAKE_TOPIC', 'earthquakes')
        self.FIRE_TOPIC = os.getenv('FIRE_TOPIC', 'fires')
        
        # Storage configuration
        self.OUTPUT_MODE = os.getenv('OUTPUT_MODE', 'console')  # console, json, parquet, hbase_rest
        self.OUTPUT_PATH = os.getenv('OUTPUT_PATH', '/tmp/flowshield_output')
        self.HBASE_REST_URL = os.getenv('HBASE_REST_URL', 'http://hadoop-master:8080')
        
        # Processing configuration
        self.CHECKPOINT_DIR = os.getenv('CHECKPOINT_DIR', './spark_checkpoints')
        self.PROCESSING_TIME = os.getenv('PROCESSING_TIME', '10 seconds')
        self.WATERMARK_THRESHOLD = os.getenv('WATERMARK_THRESHOLD', '1 hours')

class HBaseRestClient:
    """Simple HBase REST API client"""
    
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')
        
    def write_row(self, table_name, row_key, data):
        """Write a single row to HBase via REST API"""
        try:
            url = f"{self.base_url}/{table_name}/{row_key}"
            
            # Convert data to HBase REST format
            cells = []
            for key, value in data.items():
                if value is not None:
                    cells.append({
                        "column": base64.b64encode(f"info:{key}".encode()).decode(),
                        "timestamp": int(time.time() * 1000),
                        "$": base64.b64encode(str(value).encode()).decode()
                    })
            
            payload = {
                "Row": [{
                    "key": base64.b64encode(row_key.encode()).decode(),
                    "Cell": cells
                }]
            }
            
            response = requests.post(
                url, 
                json=payload, 
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            return response.status_code in [200, 201]
            
        except Exception as e:
            logger.error(f"Failed to write to HBase REST: {str(e)}")
            return False

class DataProcessor:
    """Main data processing class"""
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.spark = self._create_spark_session()
        self.hbase_client = HBaseRestClient(config.HBASE_REST_URL) if config.OUTPUT_MODE == 'hbase_rest' else None
        self._setup_schemas()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        logger.info("Initializing Spark session...")
        
        spark = SparkSession.builder \
            .appName("EarthquakeFireStreamAlternative") \
            .config("spark.sql.streaming.checkpointLocation", self.config.CHECKPOINT_DIR) \
            .config("spark.sql.streaming.schemaInference", "false") \
            .config("spark.master", "local[*]") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "localhost") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setCheckpointDir(self.config.CHECKPOINT_DIR)
        
        return spark
    
    def _setup_schemas(self):
        """Define schemas for parsing JSON data"""
        self.earthquake_schema = StructType([
            StructField("id", StringType(), False),
            StructField("time", TimestampType(), False),
            StructField("magnitude", DoubleType(), True),
            StructField("place", StringType(), True),
            StructField("url", StringType(), True),
            StructField("status", StringType(), True),
            StructField("magType", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])
        
        self.fire_schema = StructType([
            StructField("id", StringType(), False),
            StructField("time", TimestampType(), False),
            StructField("frp", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])
    
    def create_kafka_stream(self, topic: str, schema: StructType):
        """Create a Kafka stream with proper error handling"""
        logger.info(f"Creating Kafka stream for topic: {topic}")
        
        try:
            stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", "1000") \
                .load()
            
            # Parse JSON with error handling
            parsed_stream = stream.select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset")
            ).select("data.*", "kafka_timestamp", "partition", "offset") \
             .filter(col("id").isNotNull())
            
            # Add watermark for late data handling
            watermarked_stream = parsed_stream.withWatermark("time", self.config.WATERMARK_THRESHOLD)
            
            logger.info(f"Successfully created stream for {topic}")
            return watermarked_stream
            
        except Exception as e:
            logger.error(f"Failed to create stream for topic {topic}: {str(e)}")
            raise
    
    def process_earthquake_stream(self, stream):
        """Process earthquake events"""
        logger.info("Processing earthquake stream...")
        
        processed = stream \
            .withColumn("type", lit("earthquake")) \
            .withColumn("processed_time", current_timestamp()) \
            .withColumn("city", 
                when(col("place").contains(","), 
                     expr("trim(split(place, ',')[0])"))
                .otherwise(col("place"))) \
            .withColumn("country", 
                when(col("place").contains(","), 
                     expr("trim(split(place, ',')[1])"))
                .otherwise(lit("Unknown"))) \
            .withColumn("severity", 
                when(col("magnitude") < 4.0, "Low")
                .when(col("magnitude") < 6.0, "Moderate")
                .when(col("magnitude") < 7.0, "High")
                .otherwise("Extreme")) \
            .withColumn("is_valid", 
                when((col("magnitude").isNotNull()) & 
                     (col("magnitude") >= 0) & 
                     (col("magnitude") <= 10), True)
                .otherwise(False)) \
            .filter(col("is_valid") == True) \
            .drop("is_valid") \
            .withColumn("frp", lit(None).cast(DoubleType())) \
            .dropDuplicates(["id"])
        
        return processed
    
    def process_fire_stream(self, stream):
        """Process fire events"""
        logger.info("Processing fire stream...")
        
        processed = stream \
            .withColumn("type", lit("fire")) \
            .withColumn("processed_time", current_timestamp()) \
            .withColumn("severity", 
                when(col("frp") < 5, "Low")
                .when(col("frp") < 15, "Moderate")
                .when(col("frp") < 50, "High")
                .otherwise("Extreme")) \
            .withColumn("is_valid", 
                when((col("frp").isNotNull()) & 
                     (col("frp") >= 0) & 
                     (col("latitude").between(-90, 90)) &
                     (col("longitude").between(-180, 180)), True)
                .otherwise(False)) \
            .filter(col("is_valid") == True) \
            .drop("is_valid") \
            .withColumn("magnitude", lit(None).cast(DoubleType())) \
            .withColumn("place", lit(None).cast(StringType())) \
            .withColumn("url", lit(None).cast(StringType())) \
            .withColumn("status", lit(None).cast(StringType())) \
            .withColumn("magType", lit(None).cast(StringType())) \
            .dropDuplicates(["id"])
        
        return processed
    
    def write_to_console(self, stream, query_name):
        """Write stream to console for debugging"""
        return stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 20) \
            .trigger(processingTime=self.config.PROCESSING_TIME) \
            .queryName(query_name) \
            .option("checkpointLocation", f"{self.config.CHECKPOINT_DIR}/{query_name}") \
            .start()
    
    def write_to_json(self, stream, query_name, path_suffix):
        """Write stream to JSON files"""
        output_path = f"{self.config.OUTPUT_PATH}/{path_suffix}"
        
        return stream.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{self.config.CHECKPOINT_DIR}/{query_name}") \
            .trigger(processingTime=self.config.PROCESSING_TIME) \
            .queryName(query_name) \
            .start()
    
    def write_to_parquet(self, stream, query_name, path_suffix):
        """Write stream to Parquet files with partitioning"""
        output_path = f"{self.config.OUTPUT_PATH}/{path_suffix}"
        
        return stream \
            .withColumn("year", expr("year(time)")) \
            .withColumn("month", expr("month(time)")) \
            .withColumn("day", expr("day(time)")) \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{self.config.CHECKPOINT_DIR}/{query_name}") \
            .partitionBy("year", "month", "day", "type") \
            .trigger(processingTime=self.config.PROCESSING_TIME) \
            .queryName(query_name) \
            .start()
    
    def write_to_hbase_rest(self, stream, query_name, table_name):
        """Write stream to HBase via REST API"""
        
        def write_batch_to_hbase_rest(df, epoch_id):
            if df.count() > 0:
                logger.info(f"Writing batch {epoch_id} to HBase table {table_name} via REST")
                rows = df.collect()
                success_count = 0
                
                for row in rows:
                    row_data = row.asDict()
                    row_key = row_data.pop('id')
                    
                    # Remove None values
                    clean_data = {k: v for k, v in row_data.items() if v is not None}
                    
                    if self.hbase_client.write_row(table_name, row_key, clean_data):
                        success_count += 1
                
                logger.info(f"Successfully wrote {success_count}/{len(rows)} records to HBase")
        
        return stream.writeStream \
            .foreachBatch(write_batch_to_hbase_rest) \
            .outputMode("append") \
            .trigger(processingTime=self.config.PROCESSING_TIME) \
            .queryName(query_name) \
            .option("checkpointLocation", f"{self.config.CHECKPOINT_DIR}/{query_name}") \
            .start()
    
    def create_writer(self, stream, query_name, path_suffix_or_table):
        """Create appropriate writer based on output mode"""
        output_mode = self.config.OUTPUT_MODE.lower()
        
        if output_mode == 'console':
            return self.write_to_console(stream, query_name)
        elif output_mode == 'json':
            return self.write_to_json(stream, query_name, path_suffix_or_table)
        elif output_mode == 'parquet':
            return self.write_to_parquet(stream, query_name, path_suffix_or_table)
        elif output_mode == 'hbase_rest':
            return self.write_to_hbase_rest(stream, query_name, path_suffix_or_table)
        else:
            logger.warning(f"Unknown output mode: {output_mode}, defaulting to console")
            return self.write_to_console(stream, query_name)
    
    def run_streaming_pipeline(self):
        """Main method to run the streaming pipeline"""
        logger.info(f"Starting streaming pipeline with output mode: {self.config.OUTPUT_MODE}")
        
        try:
            # Create Kafka streams
            earthquake_stream = self.create_kafka_stream(
                self.config.EARTHQUAKE_TOPIC, 
                self.earthquake_schema
            )
            fire_stream = self.create_kafka_stream(
                self.config.FIRE_TOPIC, 
                self.fire_schema
            )
            
            # Process streams
            processed_earthquakes = self.process_earthquake_stream(earthquake_stream)
            processed_fires = self.process_fire_stream(fire_stream)
            
            # Create metrics stream
            metrics_stream = self.create_metrics_stream(processed_earthquakes, processed_fires)
            
            # Start writing streams based on configuration
            earthquake_query = self.create_writer(
                processed_earthquakes, 
                "earthquake_stream",
                "earthquakes" if self.config.OUTPUT_MODE in ['json', 'parquet'] else "earthquake_events"
            )
            
            fire_query = self.create_writer(
                processed_fires, 
                "fire_stream",
                "fires" if self.config.OUTPUT_MODE in ['json', 'parquet'] else "fire_events"
            )
            
            metrics_query = self.write_to_console(metrics_stream, "metrics_stream")
            
            logger.info("All streaming queries started successfully")
            logger.info(f"Output mode: {self.config.OUTPUT_MODE}")
            if self.config.OUTPUT_MODE in ['json', 'parquet']:
                logger.info(f"Output path: {self.config.OUTPUT_PATH}")
            
            # Wait for termination
            earthquake_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark.stop()
    
    def create_metrics_stream(self, earthquake_stream, fire_stream):
        """Create aggregated metrics stream"""
        logger.info("Creating metrics stream...")
        
        # Aggregate earthquake metrics
        earthquake_metrics = earthquake_stream \
            .groupBy(
                window(col("time"), "1 minute"),
                col("severity")
            ).agg(
                count("*").alias("count"),
                avg("magnitude").alias("avg_magnitude"),
                spark_max("magnitude").alias("max_magnitude")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit("earthquake").alias("event_type"),
                col("severity"),
                col("count"),
                col("avg_magnitude"),
                col("max_magnitude")
            )
        
        # Aggregate fire metrics
        fire_metrics = fire_stream \
            .groupBy(
                window(col("time"), "1 minute"),
                col("severity")
            ).agg(
                count("*").alias("count"),
                avg("frp").alias("avg_frp"),
                spark_max("frp").alias("max_frp")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit("fire").alias("event_type"),
                col("severity"),
                col("count"),
                lit(None).cast(DoubleType()).alias("avg_magnitude"),
                lit(None).cast(DoubleType()).alias("max_magnitude")
            )
        
        return earthquake_metrics.union(fire_metrics)

def main():
    """Main entry point"""
    try:
        config = StreamingConfig()
        processor = DataProcessor(config)
        
        # Print configuration
        logger.info("=== Configuration ===")
        logger.info(f"Kafka Bootstrap Servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Earthquake Topic: {config.EARTHQUAKE_TOPIC}")
        logger.info(f"Fire Topic: {config.FIRE_TOPIC}")
        logger.info(f"Output Mode: {config.OUTPUT_MODE}")
        logger.info(f"Output Path: {config.OUTPUT_PATH}")
        logger.info(f"Processing Time: {config.PROCESSING_TIME}")
        logger.info("=====================")
        
        processor.run_streaming_pipeline()
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()