# Databricks notebook source
# MAGIC %md
# MAGIC ### Aggregation functions

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/races_results")

# COMMAND ----------

demo_df = races_results_df.filter("race_year == 2020")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

# demo_df.select(count("*")).show()
# demo_df.select(count("race_name")).show()
demo_df.select(countDistinct("race_name")).show()



# COMMAND ----------

# demo_df.select(sum("points")).show()
# demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()
demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
        .withColumnRenamed("sum(points)", "total_points") \
        .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Groupby functions

# COMMAND ----------

demo_df.groupBy("driver_name") \
    .sum("points").withColumnRenamed("sum(points)", "total_points") \
    .show()

# COMMAND ----------

demo_df.groupBy("driver_name") \
    .agg(sum("points"), countDistinct("race_name").alias("number_of_races")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .show()
## when use groupBy, only 0ne aggregation function, from 02 functions => use agg() functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windows Functions

# COMMAND ----------

demo_df = races_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

grouped_df = demo_df.groupBy("race_year", "driver_name") \
    .agg(sum("points"), countDistinct("race_name").alias("number_of_races")) \
    .withColumnRenamed("sum(points)", "total_points")

# COMMAND ----------

display(grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

#driverRankSpec = Window.partitionBy("race_year").orderBy("total_points")
grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------

