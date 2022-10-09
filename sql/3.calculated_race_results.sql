-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.calc_race_results
USING PARQUET
AS
SELECT races.race_year, constructors.name AS team_name, drivers.name AS driver_name,
  results.position, results.points, 11 - results.position AS calc_points
FROM results JOIN constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN drivers ON (results.driver_id = drivers.driver_id)
  JOIN races ON (results.race_id = races.race_id)
WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calc_race_results

-- COMMAND ----------

