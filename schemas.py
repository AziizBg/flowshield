"""
Data schemas module
"""

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Schema for earthquake events
earthquake_schema = StructType([
    StructField("id", StringType(), False),
    StructField("time", TimestampType(), False),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("url", StringType(), True),
    StructField("status", StringType(), True),
    StructField("magType", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

# Schema for fire events
fire_schema = StructType([
    StructField("id", StringType(), False),
    StructField("time", TimestampType(), False),
    StructField("frp", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
]) 