-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lesson Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effects of dropping a managed table
-- MAGIC 4. DESCRIBE table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet("/mnt/formula1deltake/presentation/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

