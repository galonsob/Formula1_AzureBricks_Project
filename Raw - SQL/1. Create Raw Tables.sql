-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create DATABASE
-- MAGIC

-- COMMAND ----------

show databases

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####CREATE RAW TABLES

-- COMMAND ----------

--CREATE CIRCUIT TABLE

DROP TABLE IF EXISTS f1_raw.circuits; 
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS(path '/mnt/formula1dlgerardo/raw/circuits.csv', header 'True')

-- COMMAND ----------

-- CREATE CONSTRUCTORS TABLE

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "/mnt/formula1dlgerardo/raw/constructors.json")

-- COMMAND ----------

-- CREATE DRIVERS TABLE

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number STRING,
  code STRING,
  name STRUCT<forename: STRING, surname:STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "/mnt/formula1dlgerardo/raw/drivers.json")

-- COMMAND ----------

-- CREATE RESULTS TABLE

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestlap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json
OPTIONS(path "/mnt/formula1dlgerardo/raw/results.json")

-- COMMAND ----------

-- CREATE PIT_STOPS TABLE

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS(path "/mnt/formula1dlgerardo/raw/pit_stops.json", multiLine 'True')

-- COMMAND ----------

-- CREATE LAP TIMES TABLE (Multiple CSV Files)

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formula1dlgerardo/raw/lap_times")

-- COMMAND ----------

-- CREATE LAP TIMES TABLE (Multiple Multiline JSON Files)

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifying INT,
  raceId INT
)
USING json
OPTIONS (path "/mnt/formula1dlgerardo/raw/qualifying", multiLine 'True')
