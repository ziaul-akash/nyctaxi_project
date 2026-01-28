# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from datetime import date
from dateutil.relativedelta import relativedelta

import os
import sys

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

print(project_root)
print(sys.path)

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.transformations.metadata import add_processed_timestamp
from modules.utils.date_utils import get_target_yyyymm
# COMMAND ----------


formatted_date = get_target_yyyymm(2)

# COMMAND ----------

df= spark.read.format('parquet').load(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")


# COMMAND ----------

df = add_processed_timestamp(df)

# COMMAND ----------

df.write.mode('append').saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")