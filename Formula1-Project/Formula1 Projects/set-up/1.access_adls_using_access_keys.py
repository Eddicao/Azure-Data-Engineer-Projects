# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC 1. Set Spark Config fs.azure.account.key
# MAGIC 1. List files from demo container - use abfs driver to access data
# MAGIC 1. check result - read data circuits.csv file from Data Lake storage

# COMMAND ----------

# MAGIC %md
# MAGIC #### get the key from Access Key ADLS & set Spark config
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Access key from ADSL - formula1dl1123 - Access keys

# COMMAND ----------

### in Secure Access Section, after link Key Vault & Secret Scope - dbutils.secrets facilities
formula1_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl1123.dfs.core.windows.net",
    formula1_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1123.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1123.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



# COMMAND ----------

