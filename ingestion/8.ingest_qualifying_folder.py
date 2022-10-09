# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Qualifying JSON files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [ StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), False),
                                        StructField("constructorId", IntegerType(), False),
                                        StructField("number", IntegerType(), False),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.option("header", True) \
.option("multiline", True) \
.schema(qualifying_schema) \
.json(f"/mnt/formula1deltake/raw/{v_file_date}/qualifying/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write \
.mode("ignore") \
.parquet("/mnt/formula1deltake/processed/qualifying/")

# COMMAND ----------

