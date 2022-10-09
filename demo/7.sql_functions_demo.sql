-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] AS forename, SPLIT(name, ' ')[1] AS surname
FROM drivers

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy')
FROM drivers

-- COMMAND ----------

SELECT nationality, COUNT(*) AS num_drivers
FROM drivers
GROUP BY nationality
HAVING num_drivers > 1
ORDER BY num_drivers DESC

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers

-- COMMAND ----------

