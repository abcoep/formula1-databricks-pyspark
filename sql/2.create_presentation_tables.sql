-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1deltake/presentation/sql"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constructor_standings_df = spark.read.parquet("/mnt/formula1deltake/presentation/constructor_standings")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constructor_standings_df.printSchema()

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.race_results
(
race_name string,
race_date timestamp,
circuit_location string,
position integer,
grid integer,
race_time string,
fastest_lap integer,
points float,
driver_number string,
driver_name string,
driver_nationality string,
team string,
created_date timestamp,
race_year integer
)
USING PARQUET
LOCATION "/mnt/formula1deltake/presentation/race_results"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.driver_standings
(
race_year integer,
driver_name string,
driver_nationality string,
team string,
total_points double,
wins long,
rank integer
)
USING PARQUET
LOCATION "/mnt/formula1deltake/presentation/driver_standings"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.constructor_standings
(
race_year integer,
team string,
total_points double,
wins long,
rank integer
)
USING PARQUET
LOCATION "/mnt/formula1deltake/presentation/constructor_standings"

-- COMMAND ----------

SHOW TABLES IN f1_presentation

-- COMMAND ----------

DESC EXTENDED f1_presentation.constructor_standings

-- COMMAND ----------

DESC DATABASE EXTENDED f1_presentation

-- COMMAND ----------

