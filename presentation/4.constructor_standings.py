# Databricks notebook source
race_results = spark.read.parquet("/mnt/formula1deltake/presentation/race_results")

# COMMAND ----------

race_results.printSchema()

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

constructor_standings_df = race_results \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_ranks_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_ranks_spec))

# COMMAND ----------

display(final_df.filter("race_year=2020"))

# COMMAND ----------

final_df.write.mode("ignore").parquet("/mnt/formula1deltake/presentation/constructor_standings")

# COMMAND ----------

