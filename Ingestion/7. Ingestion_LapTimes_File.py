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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_schema = StructType(fields=[
                                    StructField('raceId',IntegerType(),False),
                                    StructField('driverId',IntegerType(),True),
                                    StructField('lap',IntegerType(),True),
                                    StructField('position',StringType(),True),
                                    StructField('time',StringType(),True),
                                    StructField('milliseconds',IntegerType(),True)
                                    ])

lap_df = spark.read.schema(lap_schema).csv(f'{raw_path}/{v_file_date}/lap_times')
#You could specify more details in the path: wildcard path
#lap_df = spark.read.schema(lap_schema).csvn('/mnt/formula1dlgerardo/raw/lap_times/lap_times_split*.csv')


# COMMAND ----------

from pyspark.sql.functions import lit

dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get("p_data_source")

lap_ingestdate_df = add_ingestion_date(lap_df)

lap_df_transformed = lap_ingestdate_df.withColumnRenamed('driverId','driver_id').withColumnRenamed('raceId','race_id')\
    .withColumn('data_source',lit(v_data_source)).withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# This was the writing for the full refresh model

#lap_df_transformed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

display(lap_df_transformed)

# COMMAND ----------

#Incremental Load
#incremental_load_overwrite('f1_processed', 'lap_times', 'race_id', lap_df_transformed)


#Incremental Load using Delta

merge_cond = 'tgt.race_id = src.race_id AND tgt.lap = src.lap AND tgt.driver_id = src.driver_id'

incremental_load_delta('f1_processed', 'lap_times', 'race_id', lap_df_transformed, processed_path, merge_cond)

# COMMAND ----------

dbutils.notebook.exit('Success')
