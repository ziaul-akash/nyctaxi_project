<div align="center">
  <h1>🚕 NYC Taxi Data Engineering Project (Azure Databricks)</h1>
</div>

<hr/>

<h2>📌 Project Overview</h2>
<p>
This project implements a modern data engineering pipeline on Azure using the Medallion Architecture (Bronze → Silver → Gold) to process NYC taxi trip data.
</p>
<p>
The pipeline ingests raw trip data, cleans and enriches it, and produces aggregated insights for analytics. It is designed to be incremental, scalable, and production-ready using Azure Databricks, Delta Lake, and Unity Catalog.
</p>

<h2>🎯 Objectives</h2>
<ul>
  <li>Build an end-to-end data pipeline using Databricks</li>
  <li>Implement incremental data processing</li>
  <li>Apply data quality checks and transformations</li>
  <li>Design Slowly Changing Dimension (SCD Type 2) logic</li>
  <li>Deliver analytics-ready datasets and external exports</li>
</ul>

<h2>🏗️ Architecture</h2>
<p align="center">
  <img src="https://raw.githubusercontent.com/ziaul-akash/nyctaxi_project/refs/heads/main/solution_architecture/part_1_medallion_architecture.png" 
       alt="Medallion Architecture" width="600"/>
</p>

<h3>Medallion Layers</h3>

<h4>🟤 Bronze Layer (nyctaxi.01_bronze)</h4>
<ul>
  <li>Stores raw ingested data from source files</li>
  <li>Source: yellow_tripdata_YYYY-MM.parquet</li>
  <li>Minimal transformation (schema enforcement, metadata columns)</li>
  <li>Table: <b>yellow_trips_raw</b></li>
</ul>

<h4>⚪ Silver Layer (nyctaxi.02_silver)</h4>
<p><b>Purpose:</b> Data cleansing + enrichment</p>

<ul>
  <li><b>yellow_trips_cleansed</b>
    <ul>
      <li>Cleaned data (null handling, type casting, filtering invalid records)</li>
    </ul>
  </li>

  <li><b>yellow_trips_enriched</b>
    <ul>
      <li>Trip duration</li>
      <li>Pickup & dropoff zones</li>
      <li>Borough mapping</li>
    </ul>
  </li>

  <li><b>taxi_zone_lookup</b>
    <ul>
      <li>Implemented as SCD Type 2</li>
      <li>Tracks historical changes using:</li>
      <li>effective_date</li>
      <li>end_date</li>
    </ul>
  </li>
</ul>

<h4>🟡 Gold Layer (nyctaxi.03_gold)</h4>
<p><b>Purpose:</b> Business-level aggregations</p>

<ul>
  <li><b>daily_trip_summary</b>
    <ul>
      <li>total trips</li>
      <li>avg passengers per trip</li>
      <li>avg distance</li>
      <li>avg fare</li>
      <li>min/max fare</li>
      <li>total revenue</li>
    </ul>
  </li>
</ul>

<h2>⚙️ Pipeline Design</h2>

<h3>🔄 Incremental Processing Logic</h3>
<ul>
  <li>Processes one month at a time</li>
  <li>Always processes data from 2 months prior (due to vendor delay)</li>
  <li>Skips:
    <ul>
      <li>Already processed data</li>
      <li>Missing data</li>
    </ul>
  </li>
  <li>Uses append-only strategy with Delta Lake</li>
</ul>

<h3>🧠 Key Transformations</h3>
<ul>
  <li>Trip duration calculation</li>
  <li>Data validation (remove invalid trips)</li>
  <li>Joins with taxi zone lookup</li>
  <li>Partitioning by vendor + month</li>
</ul>

<h2>🧩 Databricks Job Orchestration</h2>
<p>A Databricks Job orchestrates notebook tasks:</p>

<ul>
  <li>Load raw data → Bronze</li>
  <li>Cleanse data → Silver (cleansed)</li>
  <li>Enrich data → Silver (enriched)</li>
  <li>Aggregate data → Gold</li>
</ul>

<p>✔ Handles dependencies between tasks<br/>
✔ Runs on a schedule (monthly)<br/>
✔ Supports retry & failure handling</p>

<h2>📤 Data Export (External Sharing)</h2>

<h3>Requirement</h3>
<p>Export processed data for external data teams.</p>

<h3>Solution</h3>
<ul>
  <li>Export <b>yellow_trips_enriched</b> as partitioned JSON</li>
  <li>Destination: Azure Data Lake Storage Gen2</li>
  <li>Path: <code>nyctaxi_yellow_export/</code></li>
</ul>

<h3>Implementation</h3>
<ul>
  <li>Created Storage Account + Container</li>
  <li>Unity Catalog External Location</li>
  <li>External Table (nyctaxi.04_export)</li>
  <li>Data is:
    <ul>
      <li>Partitioned by vendor and month</li>
      <li>Appended monthly</li>
    </ul>
  </li>
</ul>

<h2>🧪 Technologies Used</h2>
<ul>
  <li>Azure Databricks</li>
  <li>Delta Lake</li>
  <li>Unity Catalog</li>
  <li>Azure Data Lake Storage Gen2 (ADLS)</li>
  <li>PySpark / Spark SQL</li>
  <li>Databricks Workflows (Jobs)</li>
</ul>

<h2>📊 Key Features & Highlights</h2>
<ul>
  <li>Medallion architecture implementation</li>
  <li>Incremental & idempotent pipeline</li>
  <li>SCD Type 2 dimension handling</li>
  <li>External data sharing via ADLS</li>
  <li>Partitioned data for performance</li>
  <li>Production-style orchestration</li>
</ul>

<h2>🚀 How to Run</h2>
<ul>
  <li>Upload source files (yellow_tripdata_YYYY-MM.parquet)</li>
  <li>Configure storage & Unity Catalog</li>
  <li>Run Databricks Job</li>
  <li>Monitor pipeline execution</li>
  <li>Validate output in Gold & Export layers</li>
</ul>

<h2>💡 What I Learned</h2>
<ul>
  <li>Designing scalable data lakehouse architectures</li>
  <li>Handling incremental ETL pipelines</li>
  <li>Implementing SCD Type 2 in Spark</li>
  <li>Managing data governance with Unity Catalog</li>
  <li>Building production-ready pipelines in Databricks</li>
</ul>

<h2>🧠 Short Explanation</h2>
<p>
“I built an end-to-end data pipeline on Azure Databricks using the Medallion architecture. I ingest raw NYC taxi data into Bronze, clean and enrich it in Silver—including implementing an SCD Type 2 dimension for taxi zones—and aggregate it into Gold for analytics.
</p>

<p>
The pipeline is incremental, processes one month at a time with a 2-month lag, and avoids reprocessing. I orchestrated everything using Databricks Jobs and also built an external data export pipeline to ADLS in partitioned JSON format for downstream teams.”
</p>
