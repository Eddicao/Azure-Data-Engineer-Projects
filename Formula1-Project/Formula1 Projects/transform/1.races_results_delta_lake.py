# Databricks notebook source
# MAGIC %md
# MAGIC ### join all data from 05 files [processed container] to create RACE_RESULTS file [presentation container]

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

## for incremental LOAD
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# circuits
# circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
                .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

# races
# races_df = spark.read.parquet(f"{processed_folder_path}/races") \
races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
                .withColumnRenamed("name", "race_name") \
                .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------


#drivers
## drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
                .withColumnRenamed("number", "driver_number") \
                .withColumnRenamed("name", "driver_name") \
                .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

# constructors
# constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
                    .withColumnRenamed("name", "team")

# COMMAND ----------

# results
# results_df = spark.read.parquet(f"{processed_folder_path}/results") \
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date")  ### for the data 2.driver_standings - do calculation on the entire race year

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join races with circuits

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                    .select(races_df.race_year, races_df.race_name, races_df.race_date, races_df.race_id, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join results to all other dataframe

# COMMAND ----------

races_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id ) \
                             .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                             .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = races_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date") ### for the data 2.driver_standings - do calculation on the entire race year

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'") \
                .orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/races_results")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.races_results")

# overwrite_partition(final_df, 'f1_presentation', 'races_results', 'race_id')  ### this code in Incremental load session

### For DELTA TABLE
merge_condition = "( tgt.driver_name = upd.driver_name ) AND  ( tgt.race_id = src.race_id )"
merge_delta_data(final_df, 'f1_presentation', 'races_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("stop here")

# COMMAND ----------

#%sql
# --- SHOW TABLES in f1_presentation; we need to drop the table and rerun the code to add in one column - "file_date"
# DROP TABLE f1_presentation.races_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in f1_presentation;

# COMMAND ----------

