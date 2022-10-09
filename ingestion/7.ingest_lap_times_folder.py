# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Lap Times folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), False),
                                       StructField("lap", IntegerType(), False),
                                       StructField("position", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"/mnt/formula1deltake/raw/{v_file_date}/lap_times/") # or lap_times/lap_times_split*.csv

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

