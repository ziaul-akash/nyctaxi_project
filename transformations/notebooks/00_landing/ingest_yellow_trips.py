# Databricks notebook source

import urllib.request
import shutil
import os
import sys

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import get_target_yyyymm
from modules.data_loader.file_downloader import download_file
from datetime import datetime, date,timezone
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#formatting date to download data from 2 months prior to the current date

formatted_date = get_target_yyyymm(2)


dir_path= f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

# write the file in the directory
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

try:
    #checking if the file already exist or not
    dbutils.fs.ls(local_path)
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream task")

except:
    #if the file doesnt exist then it will come here and try to download it
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

        download_file(url, dir_path, local_path)

        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File uploaded successfully in current run")

    except Exception as e:
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")