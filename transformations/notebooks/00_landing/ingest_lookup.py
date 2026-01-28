# Databricks notebook source
import urllib.request
import shutil
import os
import sys

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.data_loader.file_downloader import download_file

# COMMAND ----------

try:
    # Target URL of the public csv file to download
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    # Create the destination directory for storing the downloaded Parquet file
    dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"
    os.makedirs(dir_path, exist_ok=True) 

    # Define the full local path (including filename) where the file will be saved
    local_path = f"{dir_path}/taxi_zone_lookup.csv"

    # Write the contents of the response stream to the specified local file path
    download_file(url, dir_path, local_path)

    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File successfully downloaded")

except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File download failed: {str(e)}")
   