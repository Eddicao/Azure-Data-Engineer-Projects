# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal - 03 Nov 2023
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application from Certificates & Secrets
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------



# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-sas-token')

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl1123.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl1123.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl1123.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl1123.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl1123.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Need assign the Role - Contributor role 
# MAGIC in storage account -> access control - assign role & select member with Formula1-app

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl1123.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1123.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1123.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

