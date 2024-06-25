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

#READ

#circuits_df_firstest = spark.read.csv('dbfs:/mnt/formula1dlgerardo/raw/circuits.csv',header=True)
#circuits_df.show()
#I think in general display() is much better. You have many more options at the bottom of the result as doing graphs etc... and the fields that are more than 20 characters are not truncated

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields = [StructField('circuitId',IntegerType(),False),
                                       StructField('circuitRef',StringType(),True),
                                       StructField('name',StringType(),True),
                                       StructField('location',StringType(),True),
                                       StructField('country',StringType(),True),
                                       StructField('lat',DoubleType(),True),
                                       StructField('lng',DoubleType(),True),
                                       StructField('alt',IntegerType(),True),
                                       StructField('url',IntegerType(),True)
])

#You can also write the reading code options as:
circuits_df = spark.read.option('header',True)\
.schema(circuits_schema)\
.csv(f'{raw_path}/{v_file_date}/circuits.csv')

display(circuits_df)

# COMMAND ----------

#This could be one way of selecting just certain fields from the DF
#circuits_selected_df = circuits_df.selecct('circuitId','country')

# COMMAND ----------

#This could be another way of selecting just certain fields from the DF
#circuits_selected_df = circuits.df.select(circuits_df['circuitId'],circuits_df['country'])

# COMMAND ----------

#Finally you could also do it this way
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col('circuitId'),col('country'),col('circuitRef'),col('location')\
,col('name'),col('lat'),col('lng'),col('alt'))
#the col function is a bit more versatile as it allows you to do stuff like the following:

#circuits_selected_df = circuits.df.select(col('circuitId'),col('country').alias('race_country'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns and Add columns as required 
# MAGIC

# COMMAND ----------

##This API returns a new DF renmaing an existing column
from pyspark.sql.functions import lit


circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId','circuit_id')\
.withColumnRenamed('circuitRef','circuit_ref')\
.withColumnRenamed('lat','latitude')\
.withColumnRenamed('lng','longitude')\
.withColumnRenamed('alt','altitude')\
.withColumn('data_source',lit(v_data_source))\
.withColumn('file_date',lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add Ingestion Date to DF

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

#circuits_final_df = circuits_renamed_df.withColumn('ingestion_date',current_timestamp())


#We are going to use the function defined in the common_functions notebook to add the ingestion_date
circuits_final_df = add_ingestion_date(circuits_renamed_df)
display(circuits_final_df)

##Take into account this works because current_timestamp function adds a column-type object. However, if you wanted to add a literal value constant for all rows, for example, a new field called Environment with Production value, you would need a different function: lit

#circuits_final_df = circuits_renamed_df.withColumn('ingestion_date',current_timestamp())
#display(circuits_final_df).withColumn('Environment',lit('Production'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 5 - WRITING

# COMMAND ----------

#We are going to write our dataframe into parquet format in a particular directory
circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')
#There are many options for the mode, we choose that one so the next time we write the data it doesn't fail due to an already existing path
#Now we can read it from our file system

#df = spark.read.parquet('/mnt/formula1dlgerardo/processed/circuits')

# COMMAND ----------

dbutils.notebook.exit('Success')
