# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

## for incremental LOAD
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/races_results") \
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

from pyspark.sql.functions import col

race_year_list = df_column_to_list(races_results_df , 'race_year')

races_results_df = spark.read.parquet(f"{presentation_folder_path}/races_results") \
                        .filter(col("race_year").isin(race_year_list))
# for incremental load session - add filter to filter the data needed for processing

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct, when, col

# COMMAND ----------

constructor_standings_df = races_results_df \
            .groupBy("race_year", "team") \
            .agg( sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins") )


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructorRankSpec = Window.partitionBy("race_year").orderBy( desc("total_points"), desc("wins") )
final_df = constructor_standings_df.withColumn("rank", rank().over(constructorRankSpec))

# COMMAND ----------

# display(driver_standings_df.filter("race_year = 2020"))
display(final_df.filter("race_year = 2020"))

# COMMAND ----------

### final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')  ### this code in Incremental load session

# COMMAND ----------

dbutils.notebook.exit("stop here")