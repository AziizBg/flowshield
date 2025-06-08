# FlowShield - Real-time Natural Disaster Monitoring System

FlowShield is a real-time monitoring system that tracks and analyzes earthquake and fire events using Apache Kafka, Spark Streaming, and HBase. The system provides real-time processing, analysis, and storage of natural disaster events.

## System Architecture

The system consists of three main components:

1. **Data Producer** (`producer.py`)
   - Fetches earthquake data from USGS API
   - Fetches fire data from NASA FIRMS API
   - Publishes events to Kafka topics

2. **Data Consumer** (`consumer.py`)
   - Processes streaming data using Spark Structured Streaming
   - Implements real-time analytics and event categorization
   - Stores processed data in HBase

3. **Storage Layer** (HBase)
   - Stores processed events in two tables:
     - `earthquake_events`: Earthquake data
     - `fire_events`: Fire data

## Features

### Data Collection
- Real-time earthquake data from USGS API
- Real-time fire data from NASA FIRMS API
- Automatic data fetching every 60 seconds
- Error handling and retry mechanisms

### Data Processing
- Real-time stream processing using Spark Structured Streaming
- Event deduplication
- Severity categorization:
  - Earthquakes:
    - Low: < 4.0 magnitude
    - Moderate: 4.0-6.0 magnitude
    - High: > 6.0 magnitude
  - Fires:
    - Low: < 5 FRP (Fire Radiative Power)
    - Moderate: 5-15 FRP
    - High: > 15 FRP
- Geocoding of coordinates to city/country information
- Time-based windowing for analytics

### Data Storage
- HBase storage with REST API integration
- Column-family based storage design
- Efficient row-key design using event IDs
- Automatic table creation and management

### Monitoring and Analytics
- Real-time metrics generation
- Event count by severity
- Average and maximum magnitudes/FRP
- Time-window based aggregations

## Prerequisites

- Python 3.8+
- Apache Kafka 2.8+
- Apache Spark 3.5.0
- Apache HBase 2.4+
- Java 8+

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd flowshield
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Kafka**
   - Ensure Kafka is running on localhost:9092
   - Create topics:
     ```bash
     kafka-topics.sh --create --topic earthquakes --bootstrap-server localhost:9092
     kafka-topics.sh --create --topic fires --bootstrap-server localhost:9092
     ```

4. **Configure HBase**
   - Ensure HBase is running and accessible
   - Default REST API endpoint: http://hadoop-master:8080

## Usage

### 1. Start the Producer

```bash
python producer.py
```

The producer will:
- Connect to Kafka
- Start fetching earthquake and fire data
- Publish events to respective Kafka topics
- Log activities to console

### 2. Start the Consumer

```bash
./consumer.sh
```

The consumer will:
- Initialize Spark session
- Connect to Kafka topics
- Process incoming events
- Store data in HBase
- Generate real-time metrics

### Configuration

The system can be configured through environment variables in `consumer.sh`:

```bash
# Output configuration
export OUTPUT_MODE=hbase_rest  # Options: console, json, parquet, hbase_rest

# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export EARTHQUAKE_TOPIC=earthquakes
export FIRE_TOPIC=fires

# HBase configuration
export HBASE_REST_URL=http://hadoop-master:8080
```

## Data Schema

### Earthquake Events
- `id`: Unique identifier
- `time`: Timestamp
- `magnitude`: Earthquake magnitude
- `place`: Location description
- `latitude`: Latitude coordinate
- `longitude`: Longitude coordinate
- `severity`: Categorized severity (Low/Moderate/High)
- `city`: City name (derived from coordinates)
- `country`: Country name (derived from coordinates)

### Fire Events
- `id`: Unique identifier
- `time`: Timestamp
- `frp`: Fire Radiative Power
- `latitude`: Latitude coordinate
- `longitude`: Longitude coordinate
- `severity`: Categorized severity (Low/Moderate/High)
- `city`: City name (derived from coordinates)
- `country`: Country name (derived from coordinates)

## Monitoring

The system provides real-time monitoring through:

1. **Logging**
   - Detailed logs in `%(asctime)s - %(levelname)s - %(message)s` format
   - Log level: INFO

2. **Metrics**
   - Event counts by severity
   - Average and maximum values
   - Time-window based aggregations

## Error Handling

The system implements comprehensive error handling:

1. **Producer**
   - Automatic retry on API failures
   - Connection error handling
   - Data validation

2. **Consumer**
   - Stream processing error recovery
   - HBase connection retry
   - Data validation and cleaning

## Performance Optimization

The system is optimized for performance through:

1. **Spark Configuration**
   - Optimized batch sizes
   - Efficient state management
   - Watermark configuration for late data

2. **HBase Integration**
   - Batch writing
   - Efficient row-key design
   - Column-family optimization
