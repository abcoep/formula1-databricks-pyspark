-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT name, dob
FROM drivers
WHERE nationality = "Indian" AND dob <= "1990-01-01"
ORDER BY dob

-- COMMAND ----------

