# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal - 04 Nov 2023
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utility mount to MOUNT the storage
# MAGIC 4. Explore other file system utilities related

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3

# COMMAND ----------

### Run this code to mount again after unmount the container
dbutils.fs.mount(
  source = "abfss://demo@formula1dl1123.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl1123/demo",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl1123/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl1123/demo/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Some useful common Command

# COMMAND ----------

### See all the mounts in the workspace - run method MOUNT from DBFS
display(dbutils.fs.mounts())

# COMMAND ----------

### To remove MOUNT
dbutils.fs.unmount('/mnt/formula1dl1123/demo')

# COMMAND ----------

