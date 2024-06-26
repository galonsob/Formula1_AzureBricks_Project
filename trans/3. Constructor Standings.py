# Databricks notebook source
# MAGIC %run ../Include/Configuration

# COMMAND ----------

# MAGIC %run ../Include/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
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

team_standings_df = race_results_df.groupBy('race_year', 'team')\
.agg(sum('points').alias('Points'), count(when(col('position')==1, True)).alias('Wins'))



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

team_rank_spec = Window.partitionBy('race_year').orderBy(desc('Points'), desc('Wins'))
final_team_df = team_standings_df.withColumn('rank',rank().over(team_rank_spec))

# COMMAND ----------

display(final_team_df.filter('race_year==2020'))

# COMMAND ----------


#We comment the following line as we implement a new incremental load model#final_team_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')

#INCREMENTAL LOAD BY OVERWRITING
#incremental_load_overwrite('f1_presentation', 'constructor_standings', 'race_year', final_standings_df)


#INCREMENTAL LOAD DELTA
merge_cond = 'tgt.race_year = src.race_year AND tgt.team = src.team'

incremental_load_delta('f1_presentation', 'constructor_standings', 'race_year', final_team_df, presentation_path, merge_cond)
