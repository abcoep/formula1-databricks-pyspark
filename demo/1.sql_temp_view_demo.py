# Databricks notebook source
# MAGIC %md
# MAGIC #Access Dataframes using SQL
# MAGIC ##Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

race_results_df = spark.read.parquet("/mnt/formula1deltake/presentation/race_results")

# COMMAND ----------

# Local temp view
race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year=2020

# COMMAND ----------

p_race_year=2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year={p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results
# MAGIC WHERE race_year=2019

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

