# Databricks notebook source
from pyspark.sql.functions import count, min, max, avg, round, cast,sum
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import sys

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

print(project_root)
print(sys.path)

if project_root not in sys.path:
    sys.path.append(project_root)


from modules.utils.date_utils import get_month_start_n_months_ago
# COMMAND ----------

df= spark.read.table("nyctaxi.02_silver.taxi_trips_enriched")

# COMMAND ----------

two_months_ago= get_month_start_n_months_ago(2)
df= df.filter(df.tpep_pickup_datetime >= two_months_ago )

# COMMAND ----------

df.agg(
    max(df.tpep_pickup_datetime),
    min(df.tpep_pickup_datetime)
).display()

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

df.display()

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")