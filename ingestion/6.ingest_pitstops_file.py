# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Pitstops JSON file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

def inc_load(df, partition_col, table, output_folder):
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  if (spark._jsparkSession.catalog().tableExists(table)):
    df.write \
    .mode("overwrite") \
    .insertInto(table)
  else:
    df.write \
    .partitionBy(partition_col) \
    .format("parquet") \
    .mode("append") \
    .option("path", f"/mnt/formula1deltake/processed/{output_folder}") \
    .saveAsTable(table)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("stop", IntegerType(), False),
                                      StructField("lap", IntegerType(), False),
                                      StructField("time", StringType(), False),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pitstops_df = spark.read \
.option("header", True) \
.option("multiline", True) \
.schema(pitstops_schema) \
.json(f"/mnt/formula1deltake/raw/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_final_df = pitstops_df \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(pitstops_final_df)

# COMMAND ----------

inc_load(pitstops_final_df, "race_id", "f1_processed.pit_stops", "pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

