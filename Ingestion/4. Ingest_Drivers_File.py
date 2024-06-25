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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

name_schema = StructType(fields =[StructField('forename',StringType(),True),
                                  StructField('surname',StringType(),True)
                                  ])

drivers_schema = StructType(fields =[StructField('driverId',IntegerType(),False),
                                     StructField('driverRef',StringType(),True),
                                     StructField('number',IntegerType(),True),
                                     StructField('code',IntegerType(),True),
                                     StructField('name',name_schema),
                                     StructField('dob',DateType(),True),
                                     StructField('nationality',StringType(),True),
                                     StructField('url',StringType(),True)
                                     ])

drivers_df = spark.read.schema(drivers_schema).json(f'{raw_path}/{v_file_date}/drivers.json')

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp

drivers_ingestdate_df = add_ingestion_date(drivers_df)

drivers_transformed = drivers_ingestdate_df.drop(col('url'))\
    .withColumnRenamed('driverId','driver_id').withColumnRenamed('driverRef','driver_ref')\
    .withColumn('name',concat(col('name.forename'), lit(' '), col('name.surname')))\
    .withColumn('data_source',lit(v_data_source))\
    .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

drivers_transformed.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')
