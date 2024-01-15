# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

### We need to have race_id as the last column  - rechange the structure of table
def rearrange_partition_column(input_df, partition_column):

    column_list = []
    for col_name in input_df.schema.names:
        if col_name != partition_column:
            column_list.append(col_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

### the function to overwrite the partition
def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = rearrange_partition_column(input_df, partition_column)
    
    ## in case of Static - this will overwrite all of the data - set "dynamic" => with insertInto => will find the partitions and only replace those ones with new data received - not overwrite the entire table
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")  ## dataframe will be inserted into the table
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}") ## run for first time


# COMMAND ----------



# COMMAND ----------

### this functions use in the driver_standing files - incremental load for race_year partition

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                          .distinct() \
                          .collect()  ## get all the distinct race years - get the list
    column_value_list = [row[column_name] for row in df_row_list]

    return column_value_list

# COMMAND ----------

### This function - read incremental load - from parquet change to delta lake

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    from delta.tables import DeltaTable

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):

        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"), 
            merge_condition) \
                .whenMatchedUpdate() \
                .whenNotMatchedInsert() \
                .execute()
    
    ## output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")  ## dataframe will be inserted into the table
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}") ## run for first time
