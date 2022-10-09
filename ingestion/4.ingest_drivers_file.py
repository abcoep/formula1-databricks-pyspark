# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", StringType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema, True),
                                      StructField("dob", StringType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.option("header", True) \
.schema(drivers_schema) \
.json(f"/mnt/formula1deltake/raw/{v_file_date}/drivers.json") \
.drop("url")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

drivers_final_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write \
.mode("ignore") \
.parquet("/mnt/formula1deltake/processed/drivers")