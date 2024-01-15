# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times CSV file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the Spark Dataframe Reader

# COMMAND ----------

# MAGIC %md
# MAGIC - display(dbutils.fs.mounts())
# MAGIC - %fs
# MAGIC - ls /mnt/formula1dl1123/raw

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

## for incremental LOAD
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

## Specify a schema
lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
])
                                    

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")


# COMMAND ----------

# type(lap_times_df)
# lap_times_df.show()
display(lap_times_df)
# lap_times_df.describe().show()
#lap_times_df.printSchema()
#lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns & Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("ingestion_date", current_timestamp()) \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn('file_date', lit(v_file_date))
                                  

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "( tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id AND tgt.lap = upd.lap ) AND  ( tgt.race_id = src.race_id )"
merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dl1123/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")