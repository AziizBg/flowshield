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
    current_timestamp, when, window, sum as spark_sum, avg, max as spark_max,
    udf
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import json
import datetime
import logging
import os
import requests
import base64
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from functools import lru_cache

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
        self.PROCESSING_TIME = os.getenv('PROCESSING_TIME', '60 seconds')  # Increased to 60 seconds
        self.WATERMARK_THRESHOLD = os.getenv('WATERMARK_THRESHOLD', '2 hours')
        self.BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))  # Reduced batch size
        self.MAX_RECORDS_PER_TRIGGER = int(os.getenv('MAX_RECORDS_PER_TRIGGER', '1000'))  # Reduced max records

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

class GeoCoder:
    """Geocoding utility class with caching"""
    
    def __init__(self):
        self.geolocator = Nominatim(user_agent="flowshield")
        self.cache = {}
    
    @lru_cache(maxsize=1000)
    def get_location_info(self, lat: float, lon: float) -> tuple:
        """Get city and country from coordinates with caching"""
        try:
            location = self.geolocator.reverse(f"{lat}, {lon}", language='en')
            if location:
                address = location.raw.get('address', {})
                city = address.get('city') or address.get('town') or address.get('village') or "Unknown"
                country = address.get('country') or "Unknown"
                return city, country
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"Geocoding failed for coordinates ({lat}, {lon}): {str(e)}")
        return "Unknown", "Unknown"

class AlertManager:
    """Manages alerting functionality for events"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.alert_email = os.getenv('ALERT_EMAIL')
        self.alert_email_password = os.getenv('ALERT_EMAIL_PASSWORD')
        self.alert_recipient = os.getenv('ALERT_RECIPIENT')
        self.webhook_url = os.getenv('ALERT_WEBHOOK_URL')
        
        # Alert thresholds - including low levels for testing
        self.earthquake_thresholds = {
            'Low': 2.0,      # Alert for earthquakes >= 2.0 (for testing)
            'Moderate': 4.0,  # Alert for earthquakes >= 4.0 (for testing)
            'High': 6.0,     # Alert for earthquakes >= 6.0
            'Extreme': 7.0   # Alert for earthquakes >= 7.0
        }
        self.fire_thresholds = {
            'Low': 1.0,      # Alert for fires with FRP >= 1.0 (for testing)
            'Moderate': 5.0,  # Alert for fires with FRP >= 5.0 (for testing)
            'High': 50.0,    # Alert for fires with FRP >= 50
            'Extreme': 100.0 # Alert for fires with FRP >= 100
        }
        
        # Reduced alert cooldown for testing (1 minute instead of 5)
        self.alert_cooldown = 60  # 1 minute
        self.last_alert_time = {}
    
    def should_alert(self, event_type: str, severity: str, value: float) -> bool:
        """Determine if an alert should be sent based on severity and cooldown"""
        current_time = time.time()
        alert_key = f"{event_type}_{severity}"
        
        # Check if we're past the cooldown period
        if alert_key in self.last_alert_time:
            if current_time - self.last_alert_time[alert_key] < self.alert_cooldown:
                return False
        
        # Check if the event exceeds the threshold
        if event_type == 'earthquake':
            threshold = self.earthquake_thresholds.get(severity)
            if threshold and value >= threshold:
                self.last_alert_time[alert_key] = current_time
                return True
        elif event_type == 'fire':
            threshold = self.fire_thresholds.get(severity)
            if threshold and value >= threshold:
                self.last_alert_time[alert_key] = current_time
                return True
        
        return False
    
    def send_email_alert(self, event_type: str, severity: str, value: float, location: str):
        """Send email alert"""
        if not all([self.alert_email, self.alert_email_password, self.alert_recipient]):
            logger.warning("Email alert configuration incomplete")
            return
        
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = self.alert_email
            msg['To'] = self.alert_recipient
            msg['Subject'] = f"ALERT: {severity} {event_type.title()} Detected"
            
            body = f"""
            ALERT: {severity} {event_type.title()} Detected
            
            Details:
            - Type: {event_type}
            - Severity: {severity}
            - Value: {value}
            - Location: {location}
            - Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            This is an automated alert from FlowShield.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.alert_email, self.alert_email_password)
                server.send_message(msg)
            
            logger.info(f"Email alert sent for {event_type} {severity}")
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
    
    def send_webhook_alert(self, event_type: str, severity: str, value: float, location: str):
        """Send webhook alert"""
        if not self.webhook_url:
            logger.warning("Webhook URL not configured")
            return
        
        try:
            payload = {
                "text": f"🚨 *{severity} {event_type.title()} Alert*\n" +
                       f"*Value:* {value}\n" +
                       f"*Location:* {location}\n" +
                       f"*Time:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Webhook alert sent for {event_type} {severity}")
            else:
                logger.error(f"Failed to send webhook alert: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {str(e)}")
    
    def process_alert(self, event_type: str, severity: str, value: float, location: str):
        """Process and send alerts if conditions are met"""
        if self.should_alert(event_type, severity, value):
            self.send_email_alert(event_type, severity, value, location)
            self.send_webhook_alert(event_type, severity, value, location)

# Create a simple geocoding function
def get_location_info(lat: float, lon: float) -> tuple:
    """Get city and country from coordinates with retry logic"""
    max_retries = 3
    timeout = 10  # 10 seconds timeout
    
    for attempt in range(max_retries):
        try:
            geolocator = Nominatim(
                user_agent="flowshield",
                timeout=timeout
            )
            location = geolocator.reverse(f"{lat}, {lon}", language='en')
            if location:
                address = location.raw.get('address', {})
                city = address.get('city') or address.get('town') or address.get('village') or "Unknown"
                country = address.get('country') or "Unknown"
                return city, country
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait 1 second before retrying
                continue
            logger.warning(f"Geocoding failed for coordinates ({lat}, {lon}) after {max_retries} attempts: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during geocoding for coordinates ({lat}, {lon}): {str(e)}")
            break
    
    return "Unknown", "Unknown"

# Register the UDF
get_location = udf(get_location_info, StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True)
]))

class DataProcessor:
    """Main data processing class"""
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.spark = self._create_spark_session()
        self.hbase_client = HBaseRestClient(config.HBASE_REST_URL) if config.OUTPUT_MODE == 'hbase_rest' else None
        self.alert_manager = AlertManager()  # Initialize AlertManager
        self._setup_schemas()
        
        # Register the UDF in the Spark session
        self.spark.udf.register("get_location", get_location_info, StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]))
        
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
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "10") \
            .config("spark.sql.streaming.stateStore.rocksdb.formatVersion", "5") \
            .config("spark.sql.streaming.stateStore.rocksdb.enableStatistics", "true") \
            .config("spark.sql.streaming.stateStore.rocksdb.compression", "true") \
            .config("spark.sql.streaming.stateStore.rocksdb.blockSize", "16384") \
            .config("spark.sql.streaming.stateStore.rocksdb.cacheSize", "104857600") \
            .config("spark.sql.streaming.stateStore.rocksdb.writeBufferSize", "67108864") \
            .config("spark.sql.streaming.stateStore.rocksdb.maxWriteBufferNumber", "3") \
            .config("spark.sql.streaming.stateStore.rocksdb.minWriteBufferNumberToMerge", "2") \
            .config("spark.sql.streaming.stateStore.rocksdb.level0FileNumCompactionTrigger", "4") \
            .config("spark.sql.streaming.stateStore.rocksdb.level0SlowdownWritesTrigger", "8") \
            .config("spark.sql.streaming.stateStore.rocksdb.level0StopWritesTrigger", "12") \
            .config("spark.sql.streaming.stateStore.rocksdb.targetFileSizeBase", "67108864") \
            .config("spark.sql.streaming.stateStore.rocksdb.maxBackgroundCompactions", "2") \
            .config("spark.sql.streaming.stateStore.rocksdb.maxBackgroundFlushes", "1") \
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
                .option("maxOffsetsPerTrigger", "500") \
                .option("kafka.consumer.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
                .option("kafka.consumer.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
                .option("kafka.consumer.enable.auto.commit", "false") \
                .option("kafka.consumer.max.poll.records", "250") \
                .option("kafka.consumer.auto.offset.reset", "latest") \
                .option("kafka.consumer.group.id", f"spark-streaming-{topic}") \
                .option("kafka.consumer.client.id", f"spark-streaming-{topic}-{int(time.time())}") \
                .option("kafka.consumer.fetch.min.bytes", "1") \
                .option("kafka.consumer.fetch.max.wait.ms", "1000") \
                .option("kafka.consumer.max.partition.fetch.bytes", "524288") \
                .load()
            
            # Add debug logging for raw Kafka messages
            stream = stream.withColumn("debug_timestamp", current_timestamp())
            logger.info(f"Raw Kafka stream created for {topic}")
            
            # Parse JSON with error handling
            parsed_stream = stream.select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset"),
                col("debug_timestamp")
            ).select("data.*", "kafka_timestamp", "partition", "offset", "debug_timestamp") \
             .filter(col("id").isNotNull())
            
            # Add debug sink to show schema and sample values
            debug_schema_query = parsed_stream.writeStream \
                .format("console") \
                .outputMode("append") \
                .option("truncate", "false") \
                .option("numRows", 1) \
                .trigger(processingTime="5 seconds") \
                .queryName(f"debug_schema_{topic}") \
                .start()
            
            # Log schema information
            logger.info(f"Schema for {topic}:")
            parsed_stream.printSchema()
            
            # Log when we successfully parse messages
            parsed_stream = parsed_stream.withColumn("debug_parsed", current_timestamp())
            logger.info(f"JSON parsing configured for {topic}")
            
            # Add watermark for late data handling
            watermarked_stream = parsed_stream.withWatermark("time", self.config.WATERMARK_THRESHOLD)
            
            # Add debug sink to monitor the data
            debug_query = watermarked_stream.writeStream \
                .format("console") \
                .outputMode("append") \
                .option("truncate", "false") \
                .option("numRows", 5) \
                .trigger(processingTime="30 seconds") \
                .queryName(f"debug_{topic}") \
                .start()
            
            logger.info(f"Successfully created stream for {topic}")
            return watermarked_stream
            
        except Exception as e:
            logger.error(f"Failed to create stream for topic {topic}: {str(e)}")
            raise
    
    def _extract_coordinates_from_id(self, df):
        """Extract latitude and longitude from ID field"""
        return df \
            .withColumn("id_parts", expr("split(id, '_')")) \
            .withColumn("extracted_lat", expr("cast(id_parts[0] as double)")) \
            .withColumn("extracted_lon", expr("cast(id_parts[1] as double)"))

    def _add_common_columns(self, df, event_type):
        """Add common columns to the dataframe"""
        return df \
            .withColumn("type", lit(event_type)) \
            .withColumn("processed_time", current_timestamp())

    def _validate_coordinates(self, df):
        """Validate coordinates and add is_valid column"""
        return df \
            .withColumn("is_valid", 
                when((col("extracted_lat").isNotNull()) &
                     (col("extracted_lon").isNotNull()) &
                     (col("extracted_lat").between(-90, 90)) &
                     (col("extracted_lon").between(-180, 180)), True)
                .otherwise(False))

    def _add_location_info(self, df):
        """Add location information using geocoding"""
        return df \
            .withColumn("location_info", get_location(col("extracted_lat"), col("extracted_lon"))) \
            .withColumn("city", 
                when(col("city").isNull() | (col("city") == "Unknown"), 
                     col("location_info.city"))
                .otherwise(col("city"))) \
            .withColumn("country", 
                when(col("country").isNull() | (col("country") == "Unknown"), 
                     col("location_info.country"))
                .otherwise(col("country")))

    def _update_coordinates(self, df):
        """Update original coordinate columns with extracted values"""
        return df \
            .withColumn("latitude", col("extracted_lat")) \
            .withColumn("longitude", col("extracted_lon"))

    def _cleanup_temporary_columns(self, df):
        """Remove temporary columns used during processing"""
        return df \
            .drop("extracted_lat", "extracted_lon", "location_info", "id_parts")

    def process_earthquake_stream(self, stream):
        """Process earthquake events"""
        logger.info("Processing earthquake stream...")
        
        # Extract coordinates
        processed = self._extract_coordinates_from_id(stream)
        
        # Add common columns
        processed = self._add_common_columns(processed, "earthquake")
        
        # Add processed time
        processed = processed \
            .withColumn("processed_time", current_timestamp())
        
        # Add city and country
        processed = processed \
            .withColumn("city", 
                when(col("place").contains(","), 
                     expr("trim(split(place, ',')[0])"))
                .otherwise(col("place"))) \
            .withColumn("country", 
                when(col("place").contains(","), 
                     expr("trim(split(place, ',')[1])"))
                .otherwise(lit("Unknown")))
        
        # Add severity based on magnitude
        processed = processed \
            .withColumn("severity", 
                when(col("magnitude") < 4.0, "Low")
                .when(col("magnitude") < 6.0, "Moderate")
                .when(col("magnitude") < 7.0, "High")
                .otherwise("Extreme"))
        
        # Validate coordinates and magnitude
        processed = self._validate_coordinates(processed) \
            .withColumn("is_valid", 
                when(col("is_valid") & 
                     (col("magnitude").isNotNull()) & 
                     (col("magnitude") >= 0) & 
                     (col("magnitude") <= 10), True)
                .otherwise(False)) \
            .filter(col("is_valid") == True) \
            .drop("is_valid")
        
        # Add null columns for fire-specific fields
        processed = processed \
            .withColumn("frp", lit(None).cast(DoubleType())) \
            .withColumn("place", lit(None).cast(StringType())) \
            .withColumn("url", lit(None).cast(StringType())) \
            .withColumn("status", lit(None).cast(StringType())) \
            .withColumn("magType", lit(None).cast(StringType()))
        
        # Add location information
        processed = self._add_location_info(processed)
        
        # Update coordinates
        processed = self._update_coordinates(processed)
        
        # Cleanup and deduplicate
        processed = self._cleanup_temporary_columns(processed) \
            .dropDuplicates(["id"])
        
        return processed
    
    def process_fire_stream(self, stream):
        """Process fire events"""
        logger.info("Processing fire stream...")
        
        # Extract coordinates
        processed = self._extract_coordinates_from_id(stream)
        
        # Add common columns
        processed = self._add_common_columns(processed, "fire")
        
        # Add severity based on FRP
        processed = processed \
            .withColumn("severity", 
                when(col("frp") < 5, "Low")
                .when(col("frp") < 15, "Moderate")
                .when(col("frp") < 50, "High")
                .otherwise("Extreme"))
        
        # Validate coordinates and FRP
        processed = self._validate_coordinates(processed) \
            .withColumn("is_valid", 
                when(col("is_valid") & 
                     (col("frp").isNotNull()) & 
                     (col("frp") >= 0), True)
                .otherwise(False)) \
            .filter(col("is_valid") == True) \
            .drop("is_valid")
        
        # Add null columns for earthquake-specific fields
        processed = processed \
            .withColumn("magnitude", lit(None).cast(DoubleType())) \
            .withColumn("place", lit(None).cast(StringType())) \
            .withColumn("url", lit(None).cast(StringType())) \
            .withColumn("status", lit(None).cast(StringType())) \
            .withColumn("magType", lit(None).cast(StringType()))
        
        # Add location information
        processed = self._add_location_info(processed)
        
        # Update coordinates
        processed = self._update_coordinates(processed)
        
        # Cleanup and deduplicate
        processed = self._cleanup_temporary_columns(processed) \
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
            logger.info("Creating earthquake stream...")
            earthquake_stream = self.create_kafka_stream(
                self.config.EARTHQUAKE_TOPIC, 
                self.earthquake_schema
            )
            
            logger.info("Creating fire stream...")
            fire_stream = self.create_kafka_stream(
                self.config.FIRE_TOPIC, 
                self.fire_schema
            )
            
            # Process streams
            logger.info("Processing earthquake stream...")
            processed_earthquakes = self.process_earthquake_stream(earthquake_stream)
            
            logger.info("Processing fire stream...")
            processed_fires = self.process_fire_stream(fire_stream)
            
            # Create metrics stream before starting any queries
            logger.info("Creating metrics stream...")
            metrics_stream = self.create_metrics_stream(processed_earthquakes, processed_fires)
            
            # Add alert processing
            def process_earthquake_alerts(df, epoch_id):
                if df.count() > 0:
                    rows = df.collect()
                    for row in rows:
                        location = f"{row.city}, {row.country}"
                        self.alert_manager.process_alert(
                            event_type="earthquake",
                            severity=row.severity,
                            value=row.magnitude,
                            location=location
                        )
            
            def process_fire_alerts(df, epoch_id):
                if df.count() > 0:
                    rows = df.collect()
                    for row in rows:
                        location = f"{row.city}, {row.country}"
                        self.alert_manager.process_alert(
                            event_type="fire",
                            severity=row.severity,
                            value=row.frp,
                            location=location
                        )
            
            # Start writing streams based on configuration
            logger.info(f"Starting earthquake query with output mode: {self.config.OUTPUT_MODE}")
            earthquake_query = self.create_writer(
                processed_earthquakes.writeStream
                    .foreachBatch(process_earthquake_alerts)
                    .outputMode("append")
                    .trigger(processingTime="5 seconds"),
                "earthquake_stream",
                "earthquakes" if self.config.OUTPUT_MODE in ['json', 'parquet'] else "earthquake_events"
            )
            
            logger.info(f"Starting fire query with output mode: {self.config.OUTPUT_MODE}")
            fire_query = self.create_writer(
                processed_fires.writeStream
                    .foreachBatch(process_fire_alerts)
                    .outputMode("append")
                    .trigger(processingTime="5 seconds"),
                "fire_stream",
                "fires" if self.config.OUTPUT_MODE in ['json', 'parquet'] else "fire_events"
            )
            
            logger.info("Starting metrics query...")
            metrics_query = self.write_to_console(metrics_stream, "metrics_stream")
            
            logger.info("All streaming queries started successfully")
            logger.info(f"Output mode: {self.config.OUTPUT_MODE}")
            if self.config.OUTPUT_MODE in ['json', 'parquet']:
                logger.info(f"Output path: {self.config.OUTPUT_PATH}")
            
            # Wait for termination
            logger.info("Waiting for termination...")
            earthquake_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            logger.error("Stack trace:", exc_info=True)
            raise
        finally:
            logger.info("Stopping Spark session...")
            self.spark.stop()
    
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
                max("magnitude").alias("max_magnitude")
            ) \
            .select(
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
            .select(
                window(col("time"), "1 minute").alias("window"),
                col("severity"),
                col("frp")
            ) \
            .groupBy("window", "severity") \
            .agg(
                count("*").alias("count"),
                avg("frp").alias("avg_frp"),
                max("frp").alias("max_frp")
            ) \
            .select(
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