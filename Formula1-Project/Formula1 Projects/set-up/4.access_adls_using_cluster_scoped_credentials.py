# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set Spark Config fs.azure.account.key in the cluster - databricks-course-cluster - in advanced options - Spark config
# MAGIC 1. List files from demo container - use abfs driver to access data
# MAGIC 1. check result - read data circuits.csv file from Data Lake storage

# COMMAND ----------

# MAGIC %md
# MAGIC Access key from ADSL - formula1dl1123 - Access keys

# COMMAND ----------

### No need to get the access key, all process directly in the CLUSTER config

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1123.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1123.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



# COMMAND ----------

