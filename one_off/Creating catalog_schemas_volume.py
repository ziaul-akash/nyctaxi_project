# Databricks notebook source
spark.sql("CREATE CATALOG IF NOT EXISTS nyctaxi MANAGED LOCATION 'abfss://unity-catalog-storage@dbstoragejili5655zmh4u.dfs.core.windows.net/7405612509768625'")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.00_landing")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.01_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.02_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.03_gold")

# COMMAND ----------

spark.sql("create volume if not exists nyctaxi.00_landing.data_sources")