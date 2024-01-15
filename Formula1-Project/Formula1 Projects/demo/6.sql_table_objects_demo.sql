-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. SPARK SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE commmand
-- MAGIC 1. Find the current database
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;
--- can run the code many times with IF NOT EXISTS

-- COMMAND ----------

SHOW DATABASES;


-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

USE demo;
SELECT CURRENT_DATABASE()

-- COMMAND ----------



-- COMMAND ----------

