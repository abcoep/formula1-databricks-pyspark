-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1deltake/processed/sql"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Circuits Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_df = spark.read.parquet("/mnt/formula1deltake/processed/drivers")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_df.printSchema()

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.circuits
(
circuit_id integer,
circuit_ref string,
name string,
location string,
country string,
latitude double,
longitude double,
altitude integer,
ingestion_date timestamp,
env string
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/circuits"

-- COMMAND ----------

SELECT * FROM f1_processed.circuits

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.races
(
race_id integer,
round integer,
circuit_id integer,
name string,
race_timestamp timestamp,
ingestion_date timestamp,
race_year integer
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/races"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.drivers
(
driver_id integer,
driver_ref string,
number string,
code string,
name string,
dob string,
nationality string,
ingestion_date timestamp
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/drivers"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.constructors
(
constructor_id integer,
constructor_ref string,
name string,
nationality string,
ingestion_date timestamp
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/constructors"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.pit_stops
(
race_id integer,
driver_id integer,
stop integer,
lap integer,
time string,
duration string,
milliseconds integer,
ingestion_date timestamp
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/pit_stops"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.results
(
results_id integer,
driver_id integer,
constructor_id integer,
number integer,
grid integer,
position integer,
position_text string,
position_order integer,
points float,
laps integer,
time string,
milliseconds integer,
fastest_lap integer,
rank integer,
fastest_lap_time string,
fastest_lap_speed string,
ingestion_date timestamp,
race_id integer
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/results"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.lap_times
(
race_id integer,
driver_id integer,
lap integer,
position integer,
time string,
milliseconds integer,
ingestion_date timestamp
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/lap_times"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_processed.qualifying
(
qualify_id integer,
race_id integer,
driver_id integer,
constructor_id integer,
number integer,
position integer,
q1 string,
q2 string,
q3 string,
ingestion_date timestamp
)
USING PARQUET
LOCATION "/mnt/formula1deltake/processed/qualifying"

-- COMMAND ----------

SHOW TABLES in f1_processed;

-- COMMAND ----------

DESC EXTENDED f1_processed.qualifying

-- COMMAND ----------

DESC DATABASE EXTENDED f1_processed

-- COMMAND ----------

