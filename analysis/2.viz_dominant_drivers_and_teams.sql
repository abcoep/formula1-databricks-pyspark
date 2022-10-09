-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

 CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name, COUNT(1) AS total_races, SUM(calc_points) AS total_points, AVG(calc_points) AS avg_points,
  RANK() OVER(ORDER BY AVG(calc_points) DESC) AS driver_rank
FROM calc_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers and Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

SELECT race_year, driver_name, COUNT(1) AS total_races, SUM(calc_points) AS total_points,
  AVG(calc_points) AS avg_points
FROM calc_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name, COUNT(1) AS total_races, SUM(calc_points) AS total_points, AVG(calc_points) AS avg_points,
  RANK() OVER(ORDER BY AVG(calc_points) DESC) AS team_rank
FROM calc_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, team_name, COUNT(1) AS total_races, SUM(calc_points) AS total_points,
  AVG(calc_points) AS avg_points
FROM calc_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams where team_rank <= 10)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

