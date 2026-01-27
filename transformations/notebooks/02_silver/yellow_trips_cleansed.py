# Databricks notebook source
from pyspark.sql.functions import max, min, col, when, timestamp_diff
from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

df= spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

two_months_ago= datetime.now().replace(day=1)- relativedelta(months= 2) 
one_month_ago=datetime.now().replace(day=1)- relativedelta(months= 1) 
print(one_month_ago)

# COMMAND ----------


df.agg(
    max(df.tpep_pickup_datetime),
    min(df.tpep_pickup_datetime)
).display()

# COMMAND ----------

df= df.filter((df.tpep_pickup_datetime >= two_months_ago) & (df.tpep_pickup_datetime < one_month_ago))

# COMMAND ----------

df=df.select(
when(col("VendorID")== 1, " Creative Mobile Technologies, LLC")
    .when(col("VendorID")== 2, "Curb Mobility, LLC")
    .when(col("VendorID")== 6, "Myle Technologies Inc")
    .when(col("VendorID")== 7, "Helix")
    .otherwise("Unknown").alias("vendor"),

"tpep_pickup_datetime",
"tpep_dropoff_datetime",
timestamp_diff("MINUTE", col("tpep_pickup_datetime"), col("tpep_dropoff_datetime")).alias("trip_duration_mins"),

"passenger_count",
"trip_distance",

when(col("RatecodeID")==1, "Standard rate")
    .when(col("RatecodeID")==2, "JFK")
    .when(col("RatecodeID")==3, "Newark")
    .when(col("RatecodeID")==4, "Nassau or Westchester")
    .when(col("RatecodeID")==5, "Negotiated fare")
    .when(col("RatecodeID")==6, "Group ride")
    .otherwise("Unknown").alias("rate_type"),

"store_and_fwd_flag",

col("PULocationID").alias("pu_location_id"),
col("DOLocationID").alias("do_location_id"),

when(col("payment_type")== 0, "Flex Fare trip")
    .when(col("payment_type")== 1, "Credit card")
    .when(col("payment_type")== 2, "Cash")
    .when(col("payment_type")== 3, "No charge")
    .when(col("payment_type")== 4, "Dispute")
    .when(col("payment_type")== 6, "Voided trip")
    .otherwise("Unknown").alias("payment_type"),

"fare_amount", 
"extra",
"mta_tax",
"tip_amount",
"tolls_amount",
"improvement_surcharge",
"total_amount",
"congestion_surcharge",
"airport_fee",
"cbd_congestion_fee",
"processed_timestamp")

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")