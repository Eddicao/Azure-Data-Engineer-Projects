# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the Spark Dataframe Reader

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
pit_stops_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("stop", StringType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("duration", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
])
                                    

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")


# COMMAND ----------

# type(pit_stops_df)
# pit_stops_df.show()
# display(pit_stops_df)
# pit_stops_df.describe().show()
#pit_stops_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns & Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("ingestion_date", current_timestamp()) \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))
                                  

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "( tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id AND tgt.stop = upd.stop ) AND  ( tgt.race_id = src.race_id )"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dl1123/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops