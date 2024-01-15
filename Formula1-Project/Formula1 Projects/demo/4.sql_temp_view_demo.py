# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframes using SQL
# MAGIC #### Objectives - Local Temp View
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{raw_folder_path}/races_results")

# read a parquet file - put into dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Create a view on this dataframe - then we can run SQL on this view
# MAGIC

# COMMAND ----------

races_results_df.createOrReplaceTempView("v_races_results")
# v_races_results: a view

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Access the view from SQL Cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_races_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Access the view from Python Cell

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

df_2019 = spark.sql(f"SELECT * FROM v_races_results WHERE race_year = {p_race_year}")

## read from a view & return a dataframe

# COMMAND ----------

display(df_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can run SQL from SQL Cell & from Python Cell with spark.sql function

# COMMAND ----------

#### Objectives - Global Temp Views
1. Create Global temporary views on dataframes
2. Access the view from SQL cell
3. Access the view from Python cell
4. Access the view from ANOTHER notebook

# COMMAND ----------

races_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### can not just run the SELECT with %sql , this gv_race_results is registered in database GLOBAL_TEMP
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLE in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results;
# MAGIC --- database.table

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()