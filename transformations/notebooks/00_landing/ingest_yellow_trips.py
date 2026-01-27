# Databricks notebook source
import urllib.request
import os
import shutil
from datetime import datetime, date,timezone
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#formatting date to download data from 2 months prior to the current date
two_months_ago= date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")


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

        response = urllib.request.urlopen(url)

        # creating the directory if doesnt exist
        os.makedirs(dir_path, exist_ok=True)

        # writing the file from response to local path
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)

        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File uploaded successfully in current run")

    except Exception as e:
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")