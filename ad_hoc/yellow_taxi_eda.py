# Databricks notebook source
df= spark.read.table("nyctaxi.02_silver.taxi_trips_enriched")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Which vendor makes most revenue??

# COMMAND ----------

df.groupBy("vendor").\
    agg(
        round(sum("total_amount"), 2).alias("total_revenue")
    ).\
    orderBy("total_revenue", ascending= False).\
    display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### # which is the most popular pickup borough?

# COMMAND ----------

df.groupBy("pu_borough").\
    agg(count('*').alias("number_of_trips")).\
        orderBy("number_of_trips", ascending= False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which is the most common journey (Borough to borough)

# COMMAND ----------

df.groupBy(concat("pu_borough", lit(" --> "), "do_borough").alias("Journey")).\
    agg(count('*').alias("number_of_trips")).\
        orderBy("number_of_trips", ascending= False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a time series chart showing the number of trips and total revenue per day 

# COMMAND ----------

df_2 = spark.read.table("nyctaxi.03_gold.daily_trip_summary")

df_2.display()