-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2017
AS
SELECT race_year, driver_name, driver_nationality, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2017

-- COMMAND ----------

SELECT * FROM v_driver_standings_2017

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, driver_nationality, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2017 d_2017 JOIN v_driver_standings_2020 d_2020
ON (d_2017.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2017 d_2017 LEFT OUTER JOIN v_driver_standings_2020 d_2020
ON (d_2017.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2017 d_2017 RIGHT JOIN v_driver_standings_2020 d_2020
ON (d_2017.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2017 d_2017 FULL JOIN v_driver_standings_2020 d_2020
ON (d_2017.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2017 d_2017 SEMI JOIN v_driver_standings_2020 d_2020
ON (d_2017.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2017 d_2017 ANTI JOIN v_driver_standings_2020 d_2020
ON (d_2017.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2017 d_2017 CROSS JOIN v_driver_standings_2020 d_2020

-- COMMAND ----------

