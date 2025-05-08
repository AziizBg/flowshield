from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import datetime

# Create Spark context
sc = SparkContext(appName="EarthquakeStream")
ssc = StreamingContext(sc, 10)  # 10-second batches

# Enable checkpointing for stateful operations
ssc.checkpoint("hdfs:///checkpoints/earthquake_stream")

# Stream from socket
lines = ssc.socketTextStream("host.docker.internal", 9999)

# Function to determine fire severity
def get_fire_severity(frp):
    if frp < 5:
        return "Low"
    elif frp < 15:
        return "Moderate"
    else:
        return "High"

# Parse and categorize each event
def parse_event(record):
    try:
        event = json.loads(record)
        event_id = event.get("id")
        if not event_id:
            return (None, None)

        if event.get("type") == "fire":
            severity = get_fire_severity(float(event.get("frp")))
            fire = {
                "id": event_id,
                "time": event.get("time"),
                "frp": event.get("frp"),
                "severity": severity,
                "city": event.get("city"),
                "country": event.get("country"),
                "type": event.get("type"),
                "latitude": event.get("latitude"),
                "longitude": event.get("longitude"),
            }
            return (event_id, ("fire", json.dumps(fire)))

        elif event.get("type") == "earthquake":
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
                "type": event.get("type"),
                "status": event.get("status"),
                "magType": event.get("magType")
            }
            return (event_id, ("earthquake", json.dumps(quake)))

        else:
            return (event_id, ("others", json.dumps(event)))

    except Exception as e:
        return ("error_" + str(hash(record)), ("error", json.dumps({"error": str(e), "record": record})))

# Stateful deduplication: track if the ID has been seen
def update_state(new_values, last_state):
    if last_state is not None:
        return (last_state[0], False)  # Already seen
    elif new_values:
        return (new_values[0], True)   # First time seen
    return (None, False)

# Apply transformations
parsed_lines = lines.map(parse_event).filter(lambda x: x[0] is not None)

# Use updateStateByKey to track seen IDs and detect new ones
tracked_state = parsed_lines.updateStateByKey(update_state)

# Filter only newly seen events
new_events = tracked_state.filter(lambda x: x[1][1] is True).map(lambda x: x[1][0])

# Save categorized and filtered events
def save_partitioned(rdd):
    if not rdd.isEmpty():
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        for event_type in ["fire", "earthquake", "others", "error"]:
            filtered = rdd.filter(lambda x: x[0] == event_type).map(lambda x: x[1])
            if not filtered.isEmpty():
                filtered.saveAsTextFile(f"hdfs:///events/{event_type}/{timestamp}")

# Write output
new_events.foreachRDD(save_partitioned)

# Start streaming
ssc.start()
ssc.awaitTermination()