-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lesson Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effects of dropping a managed table
-- MAGIC 4. DESCRIBE table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet("/mnt/formula1deltake/presentation/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").mode("ignore").option("path", "/mnt/formula1deltake/presentation/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
race_name string,
race_date timestamp,
circuit_location string,
position int,
grid int,
race_time string,
fastest_lap int,
points float,
driver_number string,
driver_name string,
driver_nationality string,
team string,
created_date timestamp,
race_year int
)
USING PARQUET
LOCATION "/mnt/formula1deltake/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py where race_year = 2020

-- COMMAND ----------

SELECT COUNT(*) FROM demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

