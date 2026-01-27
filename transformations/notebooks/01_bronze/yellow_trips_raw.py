# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

two_months_ago= date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

# COMMAND ----------

df= spark.read.format('parquet').load(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")


# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

df.write.mode('append').saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")