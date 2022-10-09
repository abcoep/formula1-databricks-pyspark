# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Races CSV file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"/mnt/formula1deltake/raw/{v_file_date}/races.csv") \
.drop("url")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat, lit, current_timestamp

# COMMAND ----------

races_final_df = races_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
.drop("date", "time") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data to database as parquet

# COMMAND ----------

races_final_df.write.partitionBy("race_year").parquet("/mnt/formula1deltake/processed/races", mode="overwrite")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1deltake/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1deltake/processed/races"))

# COMMAND ----------

