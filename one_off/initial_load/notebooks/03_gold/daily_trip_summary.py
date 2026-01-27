# Databricks notebook source
df= spark.read.table("nyctaxi.02_silver.taxi_trips_enriched")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import count, min, max, avg, round, cast,sum

# COMMAND ----------

df= df.groupBy(df.tpep_pickup_datetime.cast("date").alias('pickup_date')).agg(
    count('*').alias("total_trips"),
    round(avg(df.passenger_count), 1).alias("avg_passengers_per_trip"),
    round(avg(df.trip_distance), 1).alias("avg_distance_per_trip"),
    round(avg(df.fare_amount), 2).alias("avg_fare_per_trip"),
    max(df.fare_amount).alias("max_fare"),
    min(df.fare_amount).alias("min_fare"),
    round(sum(df.total_amount),2).alias('total_revenue')
)



# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")