# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Results JSON file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), False),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), False),
                                      StructField("positionOrder", IntegerType(), False),
                                      StructField("points", FloatType(), False),
                                      StructField("laps", IntegerType(), False),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", StringType(), True),
                                      StructField("statusId", IntegerType(), False)
                                     ])

# COMMAND ----------

results_df = spark.read \
.option("header", True) \
.schema(results_schema) \
.json(f"/mnt/formula1deltake/raw/{v_file_date}/results.json") \
.drop("statusId")

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_final_df = results_df \
.withColumnRenamed("resultId", "results_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed

# COMMAND ----------

# for new_race in results_final_df.select("race_id").distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION(race_id = {new_race.race_id})")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
  results_final_df.write \
  .mode("overwrite") \
  .insertInto("f1_processed.results")
else:
  results_final_df.write \
  .partitionBy("race_id") \
  .format("parquet") \
  .mode("append") \
  .option("path", "/mnt/formula1deltake/processed/results") \
  .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

