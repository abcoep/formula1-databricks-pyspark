# Databricks notebook source
drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1deltake/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1deltake/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1deltake/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1deltake/demo";
# MAGIC 
# MAGIC USE DATABASE f1_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS drivers_merge
# MAGIC (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1deltake/demo/drivers_merge"

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO drivers_merge target
# MAGIC USING drivers_day1 src
# MAGIC ON src.driverId = target.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET dob = src.dob,
# MAGIC     forename = src.forename,
# MAGIC     surname = src.surname,
# MAGIC     updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO drivers_merge target
# MAGIC USING drivers_day2 src
# MAGIC ON src.driverId = target.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET dob = src.dob,
# MAGIC     forename = src.forename,
# MAGIC     surname = src.surname,
# MAGIC     updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

drivers_merge_df = DeltaTable.forPath(spark, '/mnt/formula1deltake/demo/drivers_merge')

drivers_merge_df.alias('target') \
  .merge(
    drivers_day3_df.alias('src'),
    'target.driverId = src.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "src.dob",
      "forename": "src.forename",
      "surname": "src.surname",
      "updatedDate": current_timestamp()
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "src.driverId",
      "dob": "src.dob",
      "forename": "src.forename",
      "surname": "src.surname",
      "createdDate": current_timestamp()
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge TIMESTAMP AS OF '2022-05-23T06:42:07.000+0000'

# COMMAND ----------

drivers_v2_df = spark.read \
  .format("delta") \
  .option("timestampAsOf", "2022-05-23T06:42:07.000+0000") \
  .load("/mnt/formula1deltake/demo/drivers_merge")

# COMMAND ----------

display(drivers_v2_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM drivers_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM drivers_merge TIMESTAMP AS OF '2022-05-23T06:42:07.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM drivers_merge WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO drivers_merge tgt
# MAGIC USING drivers_merge VERSION AS OF 3 src
# MAGIC ON tgt.driverId = src.driverId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_merge ORDER BY driverId

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS conv_table_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO conv_table_to_delta
# MAGIC SELECT * FROM drivers_merge

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1deltake/demo/conv_table_to_delta"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA conv_table_to_delta

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1deltake/demo/conv_table_to_delta/_delta_log"))

# COMMAND ----------

parquet_df = spark.table("conv_table_to_delta")

# COMMAND ----------

parquet_df.write.format("parquet").save("/mnt/formula1deltake/demo/conv_file_to_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1deltake/demo/conv_file_to_delta`