# Project Aeroflow

🛫 Real-Time Airline Delay Analytics Pipeline

<img width="1377" height="654" alt="Screenshot 2025-11-09 at 12 05 12 AM" src="https://github.com/user-attachments/assets/4bb856b7-5152-41c1-ba2d-da48f69d4ba7" />


End-to-End Data Engineering Project (Kafka + Event Hubs + Databricks + Snowflake)



 1. Project Overview

This project implements a real-time data engineering pipeline for analyzing airline delay causes using a modern cloud-native stack.
It demonstrates a streaming-first data architecture — ingesting flight delay data from a producer, streaming via Kafka + Azure Event Hubs, processing it in Apache Spark (Databricks), and finally warehousing it in Snowflake for analytics and visualization.

The pipeline supports both batch ingestion (historical data) and real-time streaming (low-latency insights).



 2. Tech Stack

Layer	Technology	Purpose
Data Ingestion	Apache Kafka + Azure Event Hubs	Real-time data streaming
Cloud Storage (Bronze Layer)	Azure Blob Storage	Raw data storage (Avro format)
Processing & Transformation	Apache Spark (Databricks)	ETL transformations for Silver & Gold layers
Data Warehouse	Snowflake	Scalable warehousing for BI and analytics
Dashboarding	Snowsight (native Snowflake UI)	Visualization and KPI dashboards




3. Architecture Workflow

+-------------------+
|   FastAPI / CSV   |
|   Local Producer  |
+---------+---------+
          |  
          v
+-------------------+
| Apache Kafka      |
| (Producer Client) |
+---------+---------+
          |
          v
+-----------------------------+
| Azure Event Hubs (Broker)  |
|  - Kafka-compatible endpoint|
|  - Two consumers:           |
|     1. Azure Blob (Capture) |
|     2. Databricks (Stream)  |
+-------------+---------------+
              |
     ---------------------
     |                   |
     v                   v
+-----------+       +----------------+
| Bronze    |       | Databricks     |
| (Avro)    |       | Spark Stream   |
+-----------+       +----------------+
     |                     |
     v                     v
+-----------------+   +--------------------+
| Silver Layer    |   | Gold Layer (KPIs)  |
| - Clean & Decode|   | - Aggregations     |
| - JSON parsing  |   | - Metrics & Trends |
+-----------------+   +--------------------+
     |                           |
     +-----------+---------------+
                 v
         +----------------+
         | Snowflake DW   |
         | (Warehouse)    |
         +----------------+
                 |
                 v
        +---------------------+
        | Snowsight Dashboard |
        | KPI & Visualization |
        +---------------------+




 4.  Data Ingestion Layer

Step 1: Local Kafka Producer
	•	A Python script (produce_send_rows.py) simulated a real-time producer.
	•	It streamed rows from Airline_Delay_Cause.csv to an Azure Event Hub.
	•	Each row was serialized as JSON and sent every 10 seconds to mimic real-world telemetry.

Key configuration:

conf = {
    "bootstrap.servers": "eh-flight-delays.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": os.environ["EVENTHUB_CONN"]
}

Step 2: Azure Event Hubs (Kafka Endpoint)
	•	Served as the Kafka-compatible broker for ingestion.
	•	Configured two consumers:
	1.	Capture to Azure Blob Storage (Bronze Layer) — enabled directly in Event Hubs GUI.
	•	Captures raw Avro files in intervals (5 min windows).
	2.	Real-time stream to Databricks — for low-latency transformations.

Purpose: decouple ingestion from processing, ensuring reliability & scalability.



 5.  Bronze Layer (Raw Data Storage)
	•	Event Hubs Capture wrote Avro-formatted messages into Azure Blob.
	•	Each message contained binary data under body — encoded JSON strings.

Example decoded record:

{
  "year": "2025",
  "month": "7",
  "carrier": "YV",
  "carrier_name": "Mesa Airlines Inc.",
  "airport": "CLT",
  "arr_flights": "134.00",
  "arr_del15": "31.00",
  "carrier_ct": "13.19",
  "weather_ct": "2.43",
  "arr_delay": "2264.00"
}




 6. Silver Layer (Data Cleaning & Structuring)

Objective:

Clean and decode Avro body → structured DataFrame → meaningful schema.

Mounting the Bronze Container:

configs = {
  "fs.azure.account.auth.type": "SAS",
  "fs.azure.sas.bronze.saflightdelays.blob.core.windows.net": "<sas-token>"
}
dbutils.fs.mount(
  source="wasbs://bronze@saflightdelays.blob.core.windows.net/",
  mount_point="/mnt/bronze",
  extra_configs=configs
)

Transformation Logic (PySpark):
	1.	Decode the Avro body field safely from bytes.
	2.	Trim corrupted JSON strings (partial capture records).
	3.	Parse JSON → structured columns using from_json().
	4.	Cast numeric fields to correct data types.

df_decoded = df_bronze.withColumn("body_str", udf_decode_trim(col("body")))
df_parsed = df_decoded.select(from_json(col("body_str"), schema).alias("data"))
df_silver = df_parsed.select("data.*")

 Result: A clean, tabular DataFrame with ~25 flight delay metrics.
Stored in Silver container in Delta format for further processing.



7. Gold Layer (Feature Engineering & KPIs)

Objective:

Aggregate and derive metrics for BI consumption — performance KPIs, delay causes, trends.

Core Metrics Engineered

KPI	Formula
delay_rate	arr_del15 / arr_flights
avg_delay_per_flight	arr_delay / arr_flights
cancel_rate	arr_cancelled / arr_flights
divert_rate	arr_diverted / arr_flights
cause_total	carrier_ct + weather_ct + nas_ct + security_ct + late_aircraft_ct
cause_percentages	individual_ct / cause_total

Aggregations
	•	Carrier-level (agg_carrier)

df.groupBy("carrier", "carrier_name", "year", "month").agg(
    sum("arr_flights").alias("total_arr_flights"),
    sum("arr_delay").alias("total_arr_delay_minutes"),
    avg("delay_rate").alias("avg_delay_per_flight")
)


	•	Monthly-level (agg_monthly)

df.groupBy("year", "month").agg(
    sum("arr_flights").alias("total_arr_flights"),
    sum("arr_del15").alias("total_arr_del15"),
    avg("delay_rate").alias("delay_rate")
)


	•	Delay Cause-level (agg_causes)

df.groupBy("carrier", "carrier_name", "year", "month").agg(
    sum("carrier_ct").alias("sum_carrier_ct"),
    sum("weather_ct").alias("sum_weather_ct"),
    sum("nas_ct").alias("sum_nas_ct"),
    sum("late_aircraft_ct").alias("sum_late_aircraft_ct")
)



Write to Gold Layer (Parquet):

agg_carrier.write.format("parquet").mode("overwrite").partitionBy("carrier","year","month").save(gold_carrier_path)

 Output stored in gold-parquet container.



8. Snowflake Integration (Data Warehousing)

Storage Integration (SAS-based)

CREATE OR REPLACE STAGE GOLD_STAGE_SAS
  URL = 'azure://saflightdelays.blob.core.windows.net/gold-parquet'
  CREDENTIALS = (AZURE_SAS_TOKEN='<sas-token>')
  FILE_FORMAT = (TYPE = PARQUET);

Table Creation

CREATE OR REPLACE TABLE GOLD_CARRIER (
  carrier STRING,
  carrier_name STRING,
  year INTEGER,
  month INTEGER,
  total_arr_flights DOUBLE,
  delay_rate DOUBLE,
  avg_delay_per_flight DOUBLE,
  ...
);

Data Load

COPY INTO GOLD_CARRIER
FROM @GOLD_STAGE_SAS
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

Four Snowflake tables created:
GOLD_MASTER, GOLD_CARRIER, GOLD_MONTHLY, GOLD_CAUSES.



9.  Dashboard & BI (Snowsight Visualization)

Using Snowflake Snowsight, a live dashboard was created.

Core KPIs
	•	 Total Flights
	•	 Delay Rate (%)
	•	 Average Delay per Flight
	•	 Cancellation Rate (%)

Visuals Created

Chart Type	Data Source	Description
Line Chart	v_monthly_trend	Delay rate and flight trends over time
Bar Chart	v_top_carriers	Top carriers by delay minutes
Pie Chart	GOLD_CAUSES	Distribution of delay causes
KPI Cards	v_overall_kpis	Total flights, delay %, average delay
Table	v_master_clean	Detailed drilldown by airport & carrier

 Dashboards built natively in Snowsight (no external BI tool required).



10.  Automation & Scalability
	•	The pipeline is designed for continuous streaming ingestion.
	•	Azure Event Hubs Capture and Databricks Auto Loader can continuously fetch incremental data.
	•	Gold → Snowflake can be scheduled via:
	•	Databricks Jobs
	•	Snowflake Tasks or Streams
	•	Airflow / ADF (future extension)


11.  Key Learnings and Highlights
	•	End-to-end real-time streaming pipeline without traditional ETL servers.
	•	Integration of Kafka + Event Hubs with Azure Blob Storage for low-cost raw data retention.
	•	PySpark in Databricks for heavy-lift transformations (Silver → Gold).
	•	Snowflake as modern cloud data warehouse enabling seamless BI integration.
	•	Snowsight dashboards for near real-time analytics directly on warehouse data.



12.  Potential Future Enhancements
	•	Implement Delta Live Tables or Auto Loader in Databricks for real-time ingestion.
	•	Add Airflow orchestration for full automation.
	•	Enable Snowflake Streams + Tasks for incremental refresh.
	•	Integrate Power BI / Streamlit in Snowflake for more interactive dashboards.
	•	Add anomaly detection (predictive delay models) in Gold layer.



13.  Outcome

 Fully functional, scalable real-time analytics pipeline demonstrating:
	•	Real-time ingestion → Streaming → Transformations → Warehousing → Visualization
	•	Modular and cloud-agnostic architecture for production-grade analytics.
	•	Real-world, multi-layer design following Bronze-Silver-Gold paradigm.

End-to-End Flow:
Producer → Kafka (Event Hubs) → Blob (Bronze) → Databricks (Silver/Gold) → Snowflake (BI)

