from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import datetime
import reverse_geocoder as rg
import pycountry

sc = SparkContext(appName="EarthquakeStream")
ssc = StreamingContext(sc, 10)  # 10-second batches

lines = ssc.socketTextStream("host.docker.internal", 9999)

def get_fire_severity(frp):
    if frp < 5:
        return "Low"
    elif frp < 15:
        return "Moderate"
    else:
        return "High"

def categorize_event(record):
    try:
        event = json.loads(record)

        if event.get("type") == "fire":
            severity = get_fire_severity(float(event.get("frp")))
            fire = {
                "id": event.get("id"),
                "time": event.get("time"),
                "frp": event.get("frp"),
                "severity": severity,
                "city": event.get("city"),
                "country": event.get("country"),
                "type": event.get("type"),
                "latitude": event.get("latitude"),
                "longitude": event.get("longitude"),
            }
            return ("fire", json.dumps(fire))

        elif event.get("type") == "earthquake":
            place = event.get("place", "")
            city, country = place.split(",") if "," in place else (place, "Unknown")
            quake = {
                "id": event.get("id"),
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
            return ("earthquake", json.dumps(quake))

        else:
            return ("others", json.dumps(event))

    except Exception as e:
        error = {
            "error": str(e),
            "record": record
        }
        return ("error", json.dumps(error))

def save_partitioned(rdd):
    if not rdd.isEmpty():
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        categorized = rdd.map(categorize_event)

        for event_type in ["fire", "earthquake", "others", "error"]:
            filtered = categorized.filter(lambda x: x[0] == event_type).map(lambda x: x[1])
            if not filtered.isEmpty():
                filtered.saveAsTextFile(f"hdfs:///events/{event_type}/{timestamp}")

lines.foreachRDD(save_partitioned)

ssc.start()
ssc.awaitTermination()
