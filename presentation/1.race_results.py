# Databricks notebook source
from pyspark.sql.functions import date_format, col

# COMMAND ----------

races_df = spark.read \
.parquet("/mnt/formula1deltake/processed/races") \
.withColumnRenamed("race_timestamp", "race_date") \
.select("race_id", "circuit_id", "race_year", "name", "race_date") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

drivers_df = spark.read \
.parquet("/mnt/formula1deltake/processed/drivers") \
.select("driver_id", "number", "name", "nationality") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

results_df = spark.read \
.parquet("/mnt/formula1deltake/processed/results") \
.select("results_id", "race_id", "driver_id", "constructor_id", "position", "grid", "time", "fastest_lap", "points") \
.withColumnRenamed("time", "race_time")

# COMMAND ----------

results_df.take(10).show()

# COMMAND ----------

constructors_df = spark.read \
.parquet("/mnt/formula1deltake/processed/constructors") \
.select("constructor_id", "name") \
.withColumnRenamed("name", "team")

# COMMAND ----------


display(constructors_df)

# COMMAND ----------

circuits_df = spark.read \
.parquet("/mnt/formula1deltake/processed/circuits") \
.select("circuit_id", "location") \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_results = races_df \
.join(circuits_df, ["circuit_id"]) \
.drop("circuit_id") \
.join(results_df, ["race_id"]) \
.drop("results_id", "race_id") \
.join(drivers_df, ["driver_id"]) \
.drop("driver_id") \
.join(constructors_df, ["constructor_id"]) \
.drop("constructor_id") \
.withColumn("created_date", current_timestamp())

# COMMAND ----------

race_results.show(10)

# COMMAND ----------

race_results.write.partitionBy("race_year").parquet("/mnt/formula1deltake/presentation/race_results", mode="overwrite")

# COMMAND ----------

