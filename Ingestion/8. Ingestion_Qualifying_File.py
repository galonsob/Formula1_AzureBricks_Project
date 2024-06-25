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

from pyspark.sql.types import StructType, StructField,StringType, IntegerType, DoubleType

qualifying_Schema = StructType(fields=[StructField('qualifyId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), True),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('constructorId', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('q1', StringType(), True),
                                    StructField('q2', StringType(), True),
                                    StructField('q3', StringType(), True)   
                                    ])

qualifying_df = spark.read.schema(qualifying_Schema).option('multiLine',True)\
    .json(f'{raw_path}/{v_file_date}/qualifying')


# COMMAND ----------

from pyspark.sql.functions import lit

dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get("p_data_source")

qualifying_ingestdate_df = add_ingestion_date(qualifying_df)

qualify_df_transformed = qualifying_ingestdate_df.withColumnRenamed('qualifyingId','qualify_id').\
     withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverID','driver_id').withColumnRenamed('constructorId','constructor_id')\
    .withColumn('data_source',lit(v_data_source)).withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# This was the writing for the full refresh model

#qualify_df_transformed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

display(qualify_df_transformed)

# COMMAND ----------

#Incremental Load
#incremental_load_overwrite('f1_processed', 'qualifying', 'race_id', qualify_df_transformed)


#Incremental Load using Delta

merge_cond = 'tgt.race_id = src.race_id AND tgt.qualifyId = src.qualifyId'

incremental_load_delta('f1_processed', 'qualifying', 'race_id', qualify_df_transformed, processed_path, merge_cond)

# COMMAND ----------

dbutils.notebook.exit('Success')
