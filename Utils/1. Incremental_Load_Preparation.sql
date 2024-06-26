-- Databricks notebook source
-- After the implementation of a full refresh load model we are going to 'go back' and get our model and ADLS ready
-- to carry out incremental loads instead

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/formula1dlgerardo/processed';

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/formula1dlgerardo/presentation';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/mnt/formula1dlgerardo/raw/', True)
