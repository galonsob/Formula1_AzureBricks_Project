-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style = "color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers and Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name, 
SUM(calculated_points) AS total_points,
COUNT(1) AS total_races,
ROUND(AVG(calculated_points),3) as avg,
RANK() OVER(ORDER BY ROUND(AVG(calculated_points),3) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1)>50
ORDER BY avg DESC

-- COMMAND ----------

SELECT * FROM v_dominant_drivers;

-- COMMAND ----------

--We join with the view just created in order to focus on just the top 10 drivers historically by point average regarding the number of races

SELECT race_year,
driver_name, 
SUM(calculated_points) AS total_points,
COUNT(1) AS total_races,
ROUND(AVG(calculated_points),3) as avg
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg DESC

-- COMMAND ----------

-- With the graphs we can see the power of visualization over tables

SELECT race_year,
driver_name, 
SUM(calculated_points) AS total_points,
COUNT(1) AS total_races,
ROUND(AVG(calculated_points),3) as avg
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####VISUALIZATION DOMINANT TEAMS

-- COMMAND ----------

--As we have 2 drivers/team we should raise the #races to 100
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name, 
SUM(calculated_points) AS total_points,
COUNT(1) AS total_races,
ROUND(AVG(calculated_points),3) as avg,
RANK() OVER(ORDER BY ROUND(AVG(calculated_points),3) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1)>100
ORDER BY avg DESC

-- COMMAND ----------

--For visualization simplicity we limit the top teams to 5
SELECT race_year,
team_name, 
SUM(calculated_points) AS total_points,
COUNT(1) AS total_races,
ROUND(AVG(calculated_points),3) as avg
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg DESC
