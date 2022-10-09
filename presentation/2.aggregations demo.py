# Databricks notebook source
race_results = spark.read.parquet("/mnt/formula1deltake/presentation/race_results")

# COMMAND ----------

demo_df = race_results.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, desc

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points").alias("totalPoints"), count("race_name").alias("totalRaces")).show()

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points").alias("total_points"), count("race_name").alias("number_of_races")) \
.orderBy(desc("total_points"), "number_of_races") \
.show()

# COMMAND ----------

demo_df = race_results.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_df \
.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("total_points"), count("race_name").alias("number_of_races")) \
.orderBy(desc("total_points"), "number_of_races")

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------

