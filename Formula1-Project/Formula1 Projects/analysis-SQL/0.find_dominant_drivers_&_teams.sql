-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Dominant Drivers

-- COMMAND ----------

---- bassed on the table calculated
SELECT  driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points

FROM f1_presentation.calculated_races_results

GROUP BY driver_name
HAVING count(1) >=50
ORDER BY total_points DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dominant team

-- COMMAND ----------

---- bassed on the table calculated
SELECT  team_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points

FROM f1_presentation.calculated_races_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING count(1) >= 100
ORDER BY total_points DESC