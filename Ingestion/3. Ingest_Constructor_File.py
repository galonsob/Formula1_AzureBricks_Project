# Databricks notebook source
# MAGIC %run ../Include/Configuration

# COMMAND ----------

# MAGIC %run ../Include/common_functions

# COMMAND ----------

#PARAMETERS
dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

#You can also define the schema this way in a DDL language style

constructor_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f'{raw_path}/{v_file_date}/constructors.json')

# COMMAND ----------

from pyspark.sql.functions import col
constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import lit

constructor_ingestdate_df = add_ingestion_date(constructor_dropped_df)

constructor_final_df = constructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
    .withColumnRenamed('constructorRef','constructor_ref') \
    .withColumn('data_source',lit(v_data_source))\
    .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructor')

# COMMAND ----------

dbutils.notebook.exit('Success')
