-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Preparation work - DROP all the tables
-- MAGIC because with new design the pipelines - we have additional columns - in processed - presentation 
-- MAGIC - DROP DATABASE with CASCADE => drop all the tables under the database
-- MAGIC - Then create again the databases (with no table inside - ready for ingestion & transformation phases)

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl1123/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dl1123/presentation";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### NEXT, TIDY UP the STORAGE ACCOUNT - ADLS in AZURE
-- MAGIC - keep empty - no data in raw / processed/ presentation containers
-- MAGIC - then load Incremental_load_data - in 03 folders based on date