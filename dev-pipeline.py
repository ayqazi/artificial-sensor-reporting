# Databricks notebook source
tables_path = "/mnt/tables/sensors/dev"

# COMMAND ----------

from pyspark.sql.types import *

json_schema = StructType([
    StructField("ambient_temperature", StringType(), False),
    StructField("humidity", StringType(), False),
    StructField("photosensor", StringType(), False),
    StructField("radiation_level", StringType(), False),
    StructField("sensor_uuid", StringType(), False),
    StructField("timestamp", LongType(), False),
])
bronze_df = spark.read.schema(json_schema).json("/mnt/sources/artificial-sensor-reporting/dev/stream-2022-07-14/")

# COMMAND ----------

grouped_by_sensor_df = (
    bronze_df
    .groupby('sensor_uuid')
    .count()
)
display(grouped_by_sensor_df)

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime

typemapped_silver_df = bronze_df.select(
    col("sensor_uuid").alias("id"),
    col("ambient_temperature").cast("decimal(5,2)"),
    col("humidity").cast("decimal(6,4)"),
    col("photosensor").cast("decimal(5,2)"),
    col("radiation_level").cast("integer"),
    from_unixtime("timestamp").cast("timestamp").alias("time"),
)
display(typemapped_silver_df)

# COMMAND ----------


