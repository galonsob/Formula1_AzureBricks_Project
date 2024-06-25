# Databricks notebook source
# MAGIC %md
# MAGIC #### READING

# COMMAND ----------

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


#races_df = spark.read.option('header',True).csv('/mnt/formula1dlgerardo/raw/races.csv')

#Now we would create our schema and then re-read our file into a new df with the right schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

raceschema = StructType(fields = [StructField('raceId', IntegerType(),False),
                                  StructField('year', IntegerType(),True),
                                  StructField('round', IntegerType(),True),
                                  StructField('circuitId', IntegerType(),True),
                                  StructField('name', StringType(),True),
                                  StructField('date', DateType(),True),
                                  StructField('time', StringType(),True),
                                  StructField('url', StringType(),True)
])
races_df = spark.read.option('header', True).schema(raceschema).csv(f'{raw_path}/{v_file_date}/races.csv')
display(races_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### TRANSFORM

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, to_timestamp

#We add a timestamp for ingestion date adn we add races_timestamp
races_ingestdate_df = add_ingestion_date(races_df).\
withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss'))\
.withColumn('data_source',lit(v_data_source))\
.withColumn('file_date',lit(v_file_date))

#We select and rename the final fields for the final df
races_final_df = races_ingestdate_df.select(col('raceId').alias('race_id'),col('circuitId').alias('circuit_id')\
,col('year').alias('race_year'),col('round'),col('name'),col('ingestion_date'),col('race_timestamp')\
,col('data_source'))

display(races_final_df)



# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE

# COMMAND ----------

races_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.races')


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgerardo/processed/races
# MAGIC

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.races;
