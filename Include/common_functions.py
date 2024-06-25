# Databricks notebook source
# ADD INGESTION DATE FUNCTION

from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date',current_timestamp())
    return output_df


# COMMAND ----------

# REORDER THE COLUMNS

def order_partition_col(df, partition_col):
    columns = df.columns  
    if partition_col not in columns:
        raise ValueError(f"Column {partition_col} does not exist within the DataFrame {df}")

    # Create new column order with the specified column at the end
    new_columns = [cols for cols in columns if cols != partition_col] + [partition_col]
    
    # Reorder DataFrame columns
    return df.select(new_columns)

# COMMAND ----------

# COLUMN TO LIST

def df_column_to_list(input_df, col_name):
    df_row_list = input_df.select(col_name).distinct().collect()

    value_list = [row[col_name] for row in df_row_list]
    return value_list

# COMMAND ----------

# INCREMENTAL LOAD by PARTITION OVERWRITING

def incremental_load_overwrite(db_name, table_name, partition_col,input_df):
    reordered_df = order_partition_col(input_df,partition_col)

    spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
    
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        reordered_df.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
    else:
        reordered_df.write.mode('overwrite').partitionBy(f'{partition_col}').format('parquet').saveAsTable(f'{db_name}.{table_name}')


# COMMAND ----------

# INCREMENTAL LOAD in Delta Lake by MERGE

def incremental_load_delta(db_name, table_name, partition_col, input_df, folder_path, merge_cond):

    spark.conf.set('spark.databricks.optimizer.dynamicPartitionPruning','true')
    from delta.tables import DeltaTable

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):

        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")

        deltaTable.alias('tgt').merge(input_df.alias('src'),\
            merge_cond)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(f'{partition_col}').format('delta').saveAsTable(f'{db_name}.{table_name}')
