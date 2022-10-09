-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

SELECT team_name, COUNT(1) AS total_races, SUM(calc_points) AS total_points, AVG(calc_points) AS avg_points,
  RANK() OVER(ORDER BY AVG(calc_points) DESC) AS team_rank
FROM calc_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

