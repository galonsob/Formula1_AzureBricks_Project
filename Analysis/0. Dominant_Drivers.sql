-- Databricks notebook source
SELECT driver_name, 
SUM(calculated_points) AS total_points,
COUNT(1) AS total_races,
ROUND(AVG(calculated_points),3) as avg
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1)>50
ORDER BY avg DESC

-- COMMAND ----------

--We can of course set a time limitation to focus the analysis on particular time spans

SELECT driver_name, 
SUM(calculated_points) AS total_points,
COUNT(1) AS total_races,
ROUND(AVG(calculated_points),3) as avg
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1)>50
ORDER BY avg DESC
