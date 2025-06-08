export OUTPUT_MODE=console
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export EARTHQUAKE_TOPIC=earthquakes
export FIRE_TOPIC=fires

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  consumer.py