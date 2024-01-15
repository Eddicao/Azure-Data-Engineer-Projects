# Databricks notebook source
# MAGIC %md
# MAGIC ### in this notebook, combine 08 notebooks to run & ingest 08 files from Raw container into Processed folder in DATA LAKE

# COMMAND ----------

### run notebook("name_of_notebook", 0, argument: send parameter to widget)
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

### dbutils.notebook.exit("success") => being run and then send back the value as string

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pitstops_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_laptimes_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast API",   "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

dbutils.notebook.exit("done here")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  race_id, count(1)
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

