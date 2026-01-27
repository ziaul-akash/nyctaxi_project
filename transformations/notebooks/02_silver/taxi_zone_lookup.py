# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, TimestampType
from datetime import datetime 
from delta.tables import DeltaTable

# COMMAND ----------

path="/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv"
df= spark.read.format('csv').option("header" , True).load(path)

# COMMAND ----------

df= df.select(
    col("LocationID").cast(IntegerType()).alias('location_id'),
    col("Borough").alias('borough'),
    col("Zone").alias('zone'),
    col("service_zone"),
    current_timestamp().alias('effective_date'),
    lit(None).cast(TimestampType()).alias("end_date")
)


# COMMAND ----------

# This logic has been included to force updates and insertions to the source taxi zone lookup data for demonstration purposes only
# THIS SHOULD NOT BE INCLUDED IN THE FINAL PROJECT CODE

from pyspark.sql.functions import *

# Insert new record to the source DataFrame
df_new = spark.createDataFrame(
    [(999, "New Borough", "New Zone", "New Service Zone")],
    schema="location_id int, borough string, zone string, service_zone string"
).withColumn("effective_date", current_timestamp()) \
 .withColumn("end_date", lit(None).cast("timestamp"))

df = df_new.union(df)

# Updating record for location_id 1
df = df.withColumn("borough", when(col("location_id")==1, "NEWARK AIRPORT").otherwise(col("borough")))

# COMMAND ----------

end_timestamp = datetime.now()

dt= DeltaTable.forName(spark, "nyctaxi.02_silver.taxi_zone_lookup")



# COMMAND ----------

#If there is any data that needs to be updated then the previous data will have a timestamp on end_date column and that means it is inactive now

dt.alias('t').merge(
    source= df.alias('s'),
    condition = "t.location_id = s.location_id AND t.end_date IS NULL AND (t.zone != s.zone OR t.borough != s.borough OR t.service_zone != s.service_zone)"
                    )\
    .whenMatchedUpdate(
        set={"end_date" : lit(end_timestamp).cast(TimestampType())}
    ).execute()

# COMMAND ----------

# lists of IDS that have been inactive
inserted_id_lists= [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}'").select('location_id').collect()]

if len(inserted_id_lists) == 0 :
    print("No new records to insert")
else: 
    dt.alias('t')\
        .merge(
            source= df.alias('s'),
            #Also possible  condition= f"s.location_id not in ({','.join(map(str, inserted_id_lists))})") best way
            # Also possible ~col("s.location_id").isin(inserted_id_lists)
            condition= f"s.location_id not in ({','.join(str(i) for i in inserted_id_lists)})").\
        whenNotMatchedInsert(
            values= {
                "t.location_id": "s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone": "s.service_zone",
                "t.effective_date": current_timestamp(),
                "t.end_date": lit(None).cast(TimestampType())
            }
        ).execute()


# COMMAND ----------

dt.alias('t')\
    .merge(
        source= df.alias('s'),
        condition= "t.location_id = s.location_id ").\
    whenNotMatchedInsert(
        values= {
            "t.location_id": "s.location_id",
            "t.borough": "s.borough",
            "t.zone": "s.zone",
            "t.service_zone": "s.service_zone",
            "t.effective_date":current_timestamp(),
            "t.end_date": lit(None).cast(TimestampType())
            }).execute()