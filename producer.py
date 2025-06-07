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
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    try:
        logger.info(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Successfully created Kafka producer")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

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
    try:
        logger.info("Starting producer...")
        producer = create_kafka_producer()
        logger.info(f"Producer started, connected to {KAFKA_BOOTSTRAP_SERVERS}")
        
        while True:
            try:
                # Fetch and publish earthquake data
                logger.info("Fetching earthquake data...")
                quakes = fetch_earthquakes()
                logger.info(f"Fetched {len(quakes)} earthquake events")
                
                for event in quakes:
                    producer.send(EARTHQUAKE_TOPIC, value=event)
                    logger.info(f"Sent earthquake: {event['id']}")

                # Fetch and publish fire data
                logger.info("Fetching fire data...")
                fires = fetch_fires()
                logger.info(f"Fetched {len(fires)} fire events")
                
                for fire in fires:
                    producer.send(FIRE_TOPIC, value=fire)
                    logger.info(f"Sent fire: {fire['id']}")
                    
                # Ensure all messages are sent before waiting
                producer.flush()
                logger.info("Waiting 60 seconds before next iteration...")
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.info("Waiting 5 seconds before retrying...")
                time.sleep(5)
                
    except Exception as e:
        logger.error(f"Fatal error in producer: {e}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting producer script...")
        start_producer()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer stopped due to error: {e}")
        raise
