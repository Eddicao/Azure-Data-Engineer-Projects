# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore the capabilities of  the dbutils secrets utilities

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')
### list the name of all the secrets we have created

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------



# COMMAND ----------

