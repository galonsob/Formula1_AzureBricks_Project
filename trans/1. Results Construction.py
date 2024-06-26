# Databricks notebook source
# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.race_results
# MAGIC

# COMMAND ----------

# MAGIC %run ../Include/Configuration

# COMMAND ----------

# MAGIC %run ../Include/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------


results_df = spark.read.format('delta').load(f'{processed_path}/results').filter(f"file_date = '{v_file_date}'")\
    .withColumnRenamed('race_id','results_race_id').withColumnRenamed('file_date','results_file_date')
races_df = spark.read.format('delta').load(f'{processed_path}/races')
circuits_df = spark.read.format('delta').load(f'{processed_path}/circuits')
constructors_df = spark.read.format('delta').load(f'{processed_path}/constructor').withColumnRenamed('name', 'team')
drivers_df = spark.read.format('delta').load(f'{processed_path}/drivers').withColumnRenamed('name', 'driver_name')

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'inner').\
    select(races_df.race_year, races_df.name, races_df.race_year , races_df.race_id, races_df.race_timestamp,\
    circuits_df.location).\
    withColumnRenamed('name','race_name').withColumnRenamed('race_timestamp','race_date').\
    withColumnRenamed('location','circuit_location')

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id, 'inner')\
    .join(constructors_df, results_df.constructor_id == constructors_df. constructor_id, 'inner')\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, 'inner')\
    .select(results_df.grid, results_df.fastest_lap_time, results_df.time, results_df.points, constructors_df.team\
    , drivers_df.nationality, drivers_df.number, drivers_df.driver_name, race_circuits_df.race_year,\
    race_circuits_df.race_name, race_circuits_df.race_date, race_circuits_df.circuit_location, results_df.position\
    ,race_circuits_df.race_id, results_df.results_file_date)\
    .withColumnRenamed('time','race_time').withColumnRenamed('number','driver_number')\
    .withColumnRenamed('nationality','driver_nationality').withColumnRenamed('results_file_date','file_date')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

race_results_final_df = race_results_df.withColumn('created_date',current_timestamp())

# COMMAND ----------

#We comment the following line as we implement a new incremental load model

#race_results_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')
###############################################

#INCREMENTAL LOAD OVERWRITING PARTITIONS
#incremental_load_overwrite('f1_presentation', 'race_results', 'race_id', race_results_final_df)

#display(race_results_final_df.filter("race_year==2020 and race_name=='Abu Dhabi Grand Prix'"))

# COMMAND ----------

#INCREMENTAL LOAD DELTA LAKE

# The driver_id would definitely be more correct that the driver_name, but for now, as we didn't include the driver_id in the df we will use the driver_name
merge_cond = 'tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name'

incremental_load_delta('f1_presentation', 'race_results', 'race_id', race_results_final_df, presentation_path, merge_cond)
