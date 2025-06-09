#!/bin/bash

# Set output mode to HBase REST API
export OUTPUT_MODE=hbase_rest

# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export EARTHQUAKE_TOPIC=earthquakes
export FIRE_TOPIC=fires

# HBase configuration
export HBASE_REST_URL=http://hadoop-master:8080

# Alert configuration
export SMTP_SERVER=smtp.gmail.com
export SMTP_PORT=587
export ALERT_EMAIL=flowshield77@gmail.com
export ALERT_EMAIL_PASSWORD=12345678A@
export ALERT_RECIPIENT=testing7102023@gmail.com
export ALERT_WEBHOOK_URL=https://hooks.slack.com/services/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

# Submit Spark job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  consumer.py