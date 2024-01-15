# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql("""
            CREATE TABLE IF NOT EXISTS f1_presentation.calculated_races_results
            (
            race_year INT,
            team_name STRING,
            driver_id INT,
            driver_name STRING,
            race_id INT,
            position INT,
            points INT,
            calculated_points INT,
            created_date TIMESTAMP,
            updated_date TIMESTAMP
            )
            USING DELTA
""")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.calculated_races_results
# MAGIC USING parquet
# MAGIC AS
# MAGIC SELECT  races.race_year,
# MAGIC         constructors.name AS team_name,
# MAGIC         drivers.name AS driver_name,
# MAGIC         results.position,
# MAGIC         results.points,
# MAGIC         11- results.position AS calculated_points
# MAGIC FROM    results
# MAGIC JOIN    drivers ON (results.driver_id = drivers.driver_id)
# MAGIC JOIN    contructors ON (results.constructor_id = contructors.constructor_id)
# MAGIC JOIN    races ON (results.race_id = races.race_id)
# MAGIC WHERE   results.position <= 10
# MAGIC
# MAGIC ---- this table can be used many time => create a TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_races_results;

# COMMAND ----------

