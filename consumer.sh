#!/bin/bash

# Set output mode to HBase REST API
export OUTPUT_MODE=hbase_rest

# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export EARTHQUAKE_TOPIC=earthquakes
export FIRE_TOPIC=fires

# HBase configuration
export HBASE_REST_URL=http://hadoop-master:8080

# Submit Spark job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  consumer.py