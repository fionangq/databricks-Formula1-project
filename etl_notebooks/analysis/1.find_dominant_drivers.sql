-- Databricks notebook source
-- MAGIC %md
-- MAGIC Should look at avg point rather than total points due to the number of races are different

-- COMMAND ----------

SELECT driver_name,
       COUNT(*) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 GROUP BY driver_name
 HAVING COUNT(*) >= 50
 ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Last decade dominant

-- COMMAND ----------

SELECT driver_name,
       COUNT(*) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2011 AND 2020
 GROUP BY driver_name
 HAVING COUNT(*) >= 50
 ORDER BY avg_points DESC

-- COMMAND ----------

SELECT driver_name,
       COUNT(*) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2010
 GROUP BY driver_name
 HAVING COUNT(*) >= 50
 ORDER BY avg_points DESC

-- COMMAND ----------


