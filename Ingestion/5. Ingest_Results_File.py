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

results_Schema = StructType(fields=[StructField('resultId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), False),
                                    StructField('constructorId', IntegerType(), False),
                                    StructField('number', IntegerType(), True),
                                    StructField('grid', IntegerType(), False),
                                    StructField('position', IntegerType(), True),
                                    StructField('positionText', StringType(), False),
                                    StructField('positionOrder', IntegerType(), False),
                                    StructField('points', DoubleType(), False),
                                    StructField('laps', IntegerType(), False),
                                    StructField('time', StringType(), True),
                                    StructField('milliseconds', IntegerType(), True),
                                    StructField('fastestLap', IntegerType(), True),
                                    StructField('rank', IntegerType(), True),
                                    StructField('fastestLapTime', StringType(), True),
                                    StructField('fastestLapSpeed', StringType(), True),
                                    StructField('statusId', IntegerType(), False)   
                                    ])

results_df = spark.read.schema(results_Schema).json(f'{raw_path}/{v_file_date}/results.json')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_ingest_date = add_ingestion_date(results_df)

results_df_transformed = results_ingest_date.withColumnRenamed('resultId','result_id').withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverID','driver_id').withColumnRenamed('constructorId','constructor_id')\
    .withColumnRenamed('positionText','position_text').withColumnRenamed('positionOrder','position_order')\
    .withColumnRenamed('fastestLap','fastest_lap_time').withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
    .drop('statusId').withColumn('data_source',lit(v_data_source)).withColumn('file_date',lit(v_file_date))

# After observing there were some duplicates coming from the source, we deduplicate our data
results_df_deduped = results_df_transformed.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

#Just in case you need to instantly and permanently remove the result files from the processed container
#dbutils.fs.rm('/mnt/formula1dlgerardo/processed/results', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1 for Incremental Load

# COMMAND ----------

#We erase the partitions (if the table exists) that we have already so we load incrementally appending the data

# for race_id_list in results_final_df.select('race_id').distinct().collect():
#     if (spark.:jsparksession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

#results_df_transformed.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2 for Incremental Load

# COMMAND ----------

# We change the overwrite mode to dynamic so when the insertInto runs it only overwrites those partitions it finds already in the table rather than overwriting the entire table regardless (static)

#spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')

# This configuration setting is already included in the implemented functions

# COMMAND ----------

# We change the order of the columns so the partition column is last as Sparks' insertInto expects it like that

# Without a function
# results_final_df = results_df_transformed.select('result_id','driver_id','constructor_id','number','grid','position'\
#     ,'position_text','position_order','points','laps','time','milliseconds','fastest_lap','rank','fastest_lap_time'\
#     ,'fastest_lap_speed','data_source','file_date','ingestion_date','race_id')

# We implement a function for better efficiency and readability

# COMMAND ----------

# Without a function
# if (spark.:jsparksession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode('overwrite').insertInto('f1_processed.results')
# else:
#     results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')



###########
## INCREMENTAL LOAD BY OVERWRITING PARTITION ##
# We use the function implemented for better efficiency and readability including the reordering one

#incremental_load_overwrite('f1_processed', 'results', 'race_id', results_df_transformed)


##########
## INCREMENTAL LOAD BY DELTA LAKE MERGE
merge_cond = 'tgt.race_id = src.race_id AND tgt.result_id = src.result_id'

incremental_load_delta('f1_processed', 'results', 'race_id', results_df_deduped, processed_path, merge_cond)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 3 for Full Refresh Mode

# COMMAND ----------


#You could easily partition this data at rest
#results_df_transformed.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.results

# COMMAND ----------

dbutils.notebook.exit('Success')
