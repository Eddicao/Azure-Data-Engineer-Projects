# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project Formula1

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

    # Set Spark Configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # unmount if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
                           
    # Mount the storage account container
    dbutils.fs.mount(
                source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                mount_point = f"/mnt/{storage_account_name}/{container_name}",
                extra_configs = configs)
    
    ### See all the mounts in the workspace - run method MOUNT from DBFS
    display(dbutils.fs.mounts())



# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Raw Container

# COMMAND ----------

mount_adls('formula1dl1123', 'raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Processed Container

# COMMAND ----------

mount_adls('formula1dl1123', 'processed')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Presentation Container

# COMMAND ----------

mount_adls('formula1dl1123', 'presentation')

# COMMAND ----------

### we check to see the mount container
dbutils.fs.ls("/mnt/formula1dl1123/demo")

# COMMAND ----------



# COMMAND ----------

