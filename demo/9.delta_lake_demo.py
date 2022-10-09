# Databricks notebook source
# MAGIC %md
# MAGIC ### Objectives
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1deltake/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferScheam", True) \
.json("/mnt/formula1deltake/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("ignore").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_managed
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1deltake/demo/results_managed"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed LIMIT 10

# COMMAND ----------

results_df.write.format("delta").mode("ignore").save("/mnt/formula1deltake/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1deltake/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external LIMIT 10

# COMMAND ----------

results_ext_df = spark.read.format("delta").load("/mnt/formula1deltake/demo/results_external")

# COMMAND ----------

display(results_ext_df)

# COMMAND ----------

results_df.write.format("delta").mode("ignore").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_partitioned
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1deltake/demo/results_partitioned"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC   WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed ORDER BY points DESC

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1deltake/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed ORDER BY points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed ORDER BY points DESC

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1deltake/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete(
  condition = "points = 0"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed WHERE ORDER BY points DESC

# COMMAND ----------

