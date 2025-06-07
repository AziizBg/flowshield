"""
Earthquake and Fire Data Producer using Kafka

This script fetches earthquake and fire data from various sources and publishes them to Kafka topics.
It runs continuously, fetching new data every minute and sending it to separate Kafka topics for
earthquakes and fires.

The producer uses the following components:
- USGS API for earthquake data
- NASA FIRMS API for fire data
- Kafka for message streaming
"""

from kafka import KafkaProducer
import json
import time
from earthquakes import fetch_earthquakes
from fires import fetch_fires

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Kafka broker address
EARTHQUAKE_TOPIC = 'earthquakes'            # Topic for earthquake events
FIRE_TOPIC = 'fires'                        # Topic for fire events

def create_kafka_producer():
    """
    Creates and returns a Kafka producer instance.
    
    The producer is configured to:
    - Connect to the specified Kafka broker
    - Automatically serialize JSON messages to bytes
    
    Returns:
        KafkaProducer: Configured Kafka producer instance
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def start_producer():
    """
    Main producer function that continuously fetches and publishes data.
    
    This function:
    1. Creates a Kafka producer
    2. Enters an infinite loop that:
       - Fetches earthquake data and publishes to the earthquakes topic
       - Fetches fire data and publishes to the fires topic
       - Waits for 60 seconds before the next iteration
    3. Handles any errors that occur during the process
    
    The function includes error handling and will retry after 5 seconds if an error occurs.
    """
    producer = create_kafka_producer()
    print(f"[Kafka] Producer started, connected to {KAFKA_BOOTSTRAP_SERVERS}")
    
    while True:
        try:
            # Fetch and publish earthquake data
            print("[Kafka] Sending earthquake data...")
            quakes = fetch_earthquakes()
            for event in quakes:
                producer.send(EARTHQUAKE_TOPIC, value=event)
                print(f"[Kafka] Sent earthquake: {event['id']}")

            # Fetch and publish fire data
            print("[Kafka] Sending fire data...")
            fires = fetch_fires()
            for fire in fires:
                producer.send(FIRE_TOPIC, value=fire)
                print(f"[Kafka] Sent fire: {fire['id']}")
                
            # Ensure all messages are sent before waiting
            producer.flush()
            time.sleep(60)  # Wait for 1 minute before next iteration
            
        except Exception as e:
            print(f"[Kafka] Error: {e}")
            time.sleep(5)  # Wait 5 seconds before retrying on error

if __name__ == "__main__":
    start_producer()
