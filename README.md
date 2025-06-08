# Real-time Disaster Data Streaming Platform

This project implements a real-time data streaming platform that collects and processes disaster-related data from multiple sources, including earthquakes, fires, and other natural disasters. The system uses Apache Kafka for message streaming and Apache Spark for real-time data processing.

## Project Structure

- `producer.py` - Main data producer that fetches and streams disaster data
- `spark_streaming_consumer.py` - Spark Streaming consumer for processing the data stream
- `fires.py` - Fire incident data processing module
- `earthquakes.py` - Earthquake data processing module
- `eonet_disasters.py` - NASA EONET disaster data processing module
- `helpers.py` - Utility functions and helper methods
- `requirements.txt` - Project dependencies

## Prerequisites

- Python 3.8+
- Apache Kafka
- Apache Spark

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd stream
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Dependencies

- kafka-python==2.0.2 - Python client for Apache Kafka
- pyspark==3.5.0 - Apache Spark Python API
- spark-streaming-kafka==1.6.3 - Kafka connector for Spark Streaming
- requests==2.31.0 - HTTP library for API calls
- geopy==2.4.1 - Geocoding library
- reverse-geocoder==1.5.1 - Reverse geocoding utilities
- pycountry==23.12.11 - Country data utilities

## Usage

1. Start Apache Kafka:
```bash
# Start Zookeeper and Kafka server
./start-kafka-zookeeper.sh
```

2. Create Kafka topics:
```bash
kafka-topics.sh --create --topic fires --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic earthquakes --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092
```

3. Start the data producer:
```bash
python producer.py
```

4. Start the Spark Streaming consumer:
```bash
spark-submit spark_streaming_consumer.py
```

## Data Sources

The system collects data from multiple sources:
- Earthquake data from USGS
- Fire incident data
- NASA EONET disaster data

## Data Processing

The Spark Streaming consumer processes the incoming data stream and performs:
- Real-time data transformation
- Geographic data enrichment
- Data validation and cleaning
- Aggregation and analysis

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- USGS for earthquake data
- NASA EONET for disaster data
- Apache Kafka and Spark communities 

## stream processes