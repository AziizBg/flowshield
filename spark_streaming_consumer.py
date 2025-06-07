"""
Earthquake and Fire Data Consumer using Spark Streaming and Kafka

This script consumes earthquake and fire data from Kafka topics, processes them using Spark Streaming,
and saves the results to HDFS. It implements deduplication and categorization of events.

The consumer:
1. Reads from two Kafka topics: 'earthquakes' and 'fires'
2. Processes each stream separately based on its type
3. Deduplicates events using stateful processing
4. Saves categorized events to HDFS

Key features:
- Real-time processing using Spark Streaming
- Stateful deduplication of events
- Separate processing for each event type
- HDFS storage with timestamp-based partitioning
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import datetime

# Create Spark session with Kafka connector
spark = SparkSession.builder \
    .appName("EarthquakeStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Get Spark context from session
sc = spark.sparkContext

# Create StreamingContext with 10-second batch interval
ssc = StreamingContext(sc, 10)

# Enable checkpointing for fault tolerance and stateful operations
ssc.checkpoint("hdfs:///checkpoints/earthquake_stream")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Kafka broker address
EARTHQUAKE_TOPIC = 'earthquakes'            # Topic for earthquake events
FIRE_TOPIC = 'fires'                        # Topic for fire events

# Create Kafka streams for both topics using structured streaming
def create_kafka_stream(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

# Create streams
earthquake_stream = create_kafka_stream(EARTHQUAKE_TOPIC)
fire_stream = create_kafka_stream(FIRE_TOPIC)

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

def parse_earthquake(record):
    """
    Parses an earthquake event from Kafka.
    
    Args:
        record (Row): Kafka message row
        
    Returns:
        tuple: (event_id, json_string) or (None, None) for invalid events
    """
    try:
        event = json.loads(record.value.decode('utf-8'))
        event_id = event.get("id")
        if not event_id:
            return (None, None)

        place = event.get("place", "")
        city, country = place.split(",") if "," in place else (place, "Unknown")
        quake = {
            "id": event_id,
            "time": event.get("time"),
            "magnitude": event.get("magnitude"),
            "city": city.strip(),
            "country": country.strip(),
            "place": event.get("place"),
            "url": event.get("url"),
            "type": "earthquake",
            "status": event.get("status"),
            "magType": event.get("magType")
        }
        return (event_id, json.dumps(quake))

    except Exception as e:
        return (None, None)

def parse_fire(record):
    """
    Parses a fire event from Kafka.
    
    Args:
        record (Row): Kafka message row
        
    Returns:
        tuple: (event_id, json_string) or (None, None) for invalid events
    """
    try:
        event = json.loads(record.value.decode('utf-8'))
        event_id = event.get("id")
        if not event_id:
            return (None, None)

        severity = get_fire_severity(float(event.get("frp")))
        fire = {
            "id": event_id,
            "time": event.get("time"),
            "frp": event.get("frp"),
            "severity": severity,
            "city": event.get("city"),
            "country": event.get("country"),
            "type": "fire",
            "latitude": event.get("latitude"),
            "longitude": event.get("longitude"),
        }
        return (event_id, json.dumps(fire))

    except Exception as e:
        return (None, None)

def update_state(new_values, last_state):
    """
    Stateful function for deduplication of events.
    
    This function:
    1. Tracks whether an event ID has been seen before
    2. Returns a tuple indicating if the event is new
    
    Args:
        new_values (list): List of new values for the key
        last_state (tuple): Previous state for the key
        
    Returns:
        tuple: (value, is_new) where is_new is a boolean
    """
    if last_state is not None:
        return (last_state[0], False)  # Already seen
    elif new_values:
        return (new_values[0], True)   # First time seen
    return (None, False)

# Process streams
def process_stream(stream, parser, event_type):
    return stream.rdd \
        .map(parser) \
        .filter(lambda x: x[0] is not None) \
        .updateStateByKey(update_state) \
        .filter(lambda x: x[1][1] is True) \
        .map(lambda x: (event_type, x[1][0]))

# Process both streams
earthquake_events = process_stream(earthquake_stream, parse_earthquake, "earthquake")
fire_events = process_stream(fire_stream, parse_fire, "fire")

# Combine the processed streams
all_events = earthquake_events.union(fire_events)

def save_partitioned(rdd):
    """
    Saves events to HDFS, partitioned by event type and timestamp.
    
    This function:
    1. Checks if the RDD is not empty
    2. Creates a timestamp for the batch
    3. Saves events to separate directories based on their type
    4. Uses HDFS path structure: /events/{event_type}/{timestamp}
    
    Args:
        rdd (RDD): Resilient Distributed Dataset containing the events
    """
    if not rdd.isEmpty():
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        for event_type in ["fire", "earthquake"]:
            filtered = rdd.filter(lambda x: x[0] == event_type).map(lambda x: x[1])
            if not filtered.isEmpty():
                filtered.saveAsTextFile(f"hdfs:///events/{event_type}/{timestamp}")

# Apply the save function to each RDD in the stream
all_events.foreachRDD(save_partitioned)

# Start the streaming context
ssc.start()
# Wait for the streaming context to terminate
ssc.awaitTermination()