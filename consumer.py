"""
Earthquake and Fire Data Consumer - Modular Version

This version uses a modular approach with separate components for:
1. Configuration
2. HBase Client
3. GeoCoder
4. Alert Manager
5. Data Schemas
6. Main Processing Logic

Run with:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer.py
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, struct, lit, to_json, count, 
    current_timestamp, when, window, sum as spark_sum, avg, max as spark_max,
    udf
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from config import StreamingConfig
from hbase_client import HBaseRestClient
from geocoder import GeoCoder
from alert_manager import AlertManager
from schemas import earthquake_schema, fire_schema

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Main data processing class"""
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.spark = self._create_spark_session()
        self.hbase_client = HBaseRestClient(config.HBASE_REST_URL) if config.OUTPUT_MODE == 'hbase_rest' else None
        self.alert_manager = AlertManager()
        self.geocoder = GeoCoder()
        
        # Register the UDF
        self.spark.udf.register("get_location", self.geocoder.get_location_info, StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]))
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        logger.info("Initializing Spark session...")
        
        spark = SparkSession.builder \
            .appName("EarthquakeFireStream") \
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
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "500") \
            .config("spark.streaming.kafka.consumer.cache.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.streaming.concurrentJobs", "2") \
            .config("spark.streaming.receiver.maxRate", "500") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
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
                .option("maxOffsetsPerTrigger", str(self.config.MAX_RECORDS_PER_TRIGGER)) \
                .load()
            
            parsed_stream = stream.select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp") \
             .filter(col("id").isNotNull())
            
            return parsed_stream.withWatermark("time", self.config.WATERMARK_THRESHOLD)
            
        except Exception as e:
            logger.error(f"Failed to create stream for topic {topic}: {str(e)}")
            raise
    
    def process_earthquake_stream(self, stream):
        """Process earthquake events"""
        logger.info("Processing earthquake stream...")
        
        return stream \
            .withColumn("type", lit("earthquake")) \
            .withColumn("severity", 
                when(col("magnitude") < 4.0, "Low")
                .when(col("magnitude") < 6.0, "Moderate")
                .when(col("magnitude") < 7.0, "High")
                .otherwise("Extreme")) \
            .withColumn("location_info", udf(self.geocoder.get_location_info)(col("latitude"), col("longitude"))) \
            .withColumn("city", col("location_info.city")) \
            .withColumn("country", col("location_info.country")) \
            .drop("location_info")
    
    def process_fire_stream(self, stream):
        """Process fire events"""
        logger.info("Processing fire stream...")
        
        return stream \
            .withColumn("type", lit("fire")) \
            .withColumn("severity", 
                when(col("frp") < 5, "Low")
                .when(col("frp") < 15, "Moderate")
                .when(col("frp") < 50, "High")
                .otherwise("Extreme")) \
            .withColumn("location_info", udf(self.geocoder.get_location_info)(col("latitude"), col("longitude"))) \
            .withColumn("city", 
                when(col("city").isNull(), col("location_info.city"))
                .otherwise(col("city"))) \
            .withColumn("country", 
                when(col("country").isNull(), col("location_info.country"))
                .otherwise(col("country"))) \
            .drop("location_info")
    
    def create_metrics_stream(self, earthquake_stream, fire_stream):
        """Create aggregated metrics stream"""
        logger.info("Creating metrics stream...")
        
        # Aggregate earthquake metrics
        earthquake_metrics = earthquake_stream \
            .select(
                window(col("time"), "1 minute").alias("window"),
                col("severity"),
                col("magnitude")
            ) \
            .groupBy("window", "severity") \
            .agg(
                count("*").alias("count"),
                avg("magnitude").alias("avg_magnitude"),
                expr("max(magnitude)").alias("max_magnitude")
            )
        
        # Aggregate fire metrics
        fire_metrics = fire_stream \
            .select(
                window(col("time"), "1 minute").alias("window"),
                col("severity"),
                col("frp")
            ) \
            .groupBy("window", "severity") \
            .agg(
                count("*").alias("count"),
                avg("frp").alias("avg_frp"),
                expr("max(frp)").alias("max_frp")
            )
        
        return earthquake_metrics.union(fire_metrics)
    
    def run_streaming_pipeline(self):
        """Main method to run the streaming pipeline"""
        logger.info(f"Starting streaming pipeline with output mode: {self.config.OUTPUT_MODE}")
        
        try:
            # Create and process streams
            earthquake_stream = self.process_earthquake_stream(
                self.create_kafka_stream(self.config.EARTHQUAKE_TOPIC, earthquake_schema)
            )
            
            fire_stream = self.process_fire_stream(
                self.create_kafka_stream(self.config.FIRE_TOPIC, fire_schema)
            )
            
            # Create metrics stream
            metrics_stream = self.create_metrics_stream(earthquake_stream, fire_stream)
            
            # Process alerts
            def process_earthquake_alerts(df, epoch_id):
                if df.count() > 0:
                    for row in df.collect():
                        location = f"{row.city}, {row.country}"
                        self.alert_manager.process_alert(
                            event_type="earthquake",
                            severity=row.severity,
                            value=row.magnitude,
                            location=location
                        )
            
            def process_fire_alerts(df, epoch_id):
                if df.count() > 0:
                    for row in df.collect():
                        location = f"{row.city}, {row.country}"
                        self.alert_manager.process_alert(
                            event_type="fire",
                            severity=row.severity,
                            value=row.frp,
                            location=location
                        )
            
            # Start queries
            earthquake_query = earthquake_stream.writeStream \
                .foreachBatch(process_earthquake_alerts) \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 20) \
                .trigger(processingTime="5 seconds") \
                .queryName("earthquake_stream") \
                .start()
            
            fire_query = fire_stream.writeStream \
                .foreachBatch(process_fire_alerts) \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 20) \
                .trigger(processingTime="5 seconds") \
                .queryName("fire_stream") \
                .start()
            
            metrics_query = metrics_stream.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 20) \
                .trigger(processingTime=self.config.PROCESSING_TIME) \
                .queryName("metrics_stream") \
                .start()
            
            logger.info("All streaming queries started successfully")
            
            # Wait for termination
            earthquake_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            logger.error("Stack trace:", exc_info=True)
            raise
        finally:
            logger.info("Stopping Spark session...")
            self.spark.stop()

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