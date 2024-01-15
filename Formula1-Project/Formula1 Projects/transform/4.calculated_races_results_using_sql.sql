-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.calculated_races_results
USING parquet
AS
SELECT  races.race_year,
        constructors.name AS team_name,
        drivers.name AS driver_name,
        results.position,
        results.points,
        11- results.position AS calculated_points
FROM    results
JOIN    drivers ON (results.driver_id = drivers.driver_id)
JOIN    contructors ON (results.constructor_id = contructors.constructor_id)
JOIN    races ON (results.race_id = races.race_id)
WHERE   results.position <= 10

---- this table can be used many time => create a TABLE

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_races_results;

-- COMMAND ----------

