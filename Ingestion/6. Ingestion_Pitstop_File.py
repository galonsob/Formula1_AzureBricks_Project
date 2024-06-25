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

pitstop_schema = StructType(fields=[StructField('driverId',IntegerType(),True),
                                    StructField('raceId',IntegerType(),False),
                                    StructField('stop',StringType(),True),
                                    StructField('lap',IntegerType(),True),
                                    StructField('time',StringType(),True),
                                    StructField('duration',StringType(),True),
                                    StructField('milliseconds',IntegerType(),True)
                                    ])

pitstop_df = spark.read.schema(pitstop_schema).option('multiLine',True).json(f'{raw_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import lit

pitstop_ingestdate_df = add_ingestion_date(pitstop_df)

pitstop_df_transformed = pitstop_ingestdate_df.withColumnRenamed('driverId','driver_id').withColumnRenamed('raceId','race_id')\
    .withColumn('data_source',lit(v_data_source)).withColumn('file_date',lit(v_file_date))

# COMMAND ----------

display(pitstop_df_transformed)

# COMMAND ----------

# This was the writing for the full refresh model

#pitstop_df_transformed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pitstop')

# COMMAND ----------

# Incremental Load
#incremental_load_overwrite('f1_processed', 'pitstop', 'race_id', pitstop_df_transformed)


#Incremental Load using Delta

merge_cond = 'tgt.race_id = src.race_id AND tgt.stop = src.stop AND tgt.driver_id = src.driver_id'

incremental_load_delta('f1_processed', 'pitstop', 'race_id', pitstop_df_transformed, processed_path, merge_cond)

# COMMAND ----------

dbutils.notebook.exit('Success')
