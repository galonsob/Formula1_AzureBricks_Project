# Databricks notebook source
# MAGIC %run ../Include/Configuration

# COMMAND ----------

# MAGIC %run ../Include/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

#Find years for which the data is going to be reprocessed
results_df = spark.read.format('delta').load(f'{presentation_path}/race_results').filter(f"file_date = '{v_file_date}'")

#List of the race years
race_year_list = df_column_to_list(results_df, 'race_year')

#If the year is included in the list obtained above then it will be included in the race_results_df
race_results_df = spark.read.format('delta').load(f'{presentation_path}/race_results').\
    filter(col("race_year").isin(race_year_list))

driver_standings_df = race_results_df.groupBy('race_year', 'driver_name', 'driver_nationality')\
.agg(sum('points').alias('total_points'), count(when(col('position')==1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_standings_df = driver_standings_df.withColumn('rank',rank().over(driver_rank_spec))

# COMMAND ----------

#We comment the following line as we implement a new incremental load model
#final_standings_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')

#INCREMENTAL LOAD by OVERWRITING
#incremental_load_overwrite('f1_presentation', 'driver_standings', 'race_year', final_standings_df)

#INCREMENTAL LOAD DELTA
merge_cond = 'tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name'

incremental_load_delta('f1_presentation', 'driver_standings', 'race_year', final_standings_df, presentation_path, merge_cond)

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.driver_standingd
