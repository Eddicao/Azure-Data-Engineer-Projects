# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying JSON file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the Spark Dataframe Reader

# COMMAND ----------

# MAGIC %md
# MAGIC - display(dbutils.fs.mounts())
# MAGIC - %fs
# MAGIC - ls /mnt/formula1dl1123/raw

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

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
qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)
])
                                    

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")


# COMMAND ----------

# type(qualifying_df)
# qualifying_df.show()
display(qualifying_df)
# qualifying_df.describe().show()
#qualifying_df.printSchema()
#qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns & Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("ingestion_date", current_timestamp()) \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))
                                  

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

#  final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "( tgt.qualify_id = upd.qualify_id ) AND  ( tgt.race_id = src.race_id )"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dl1123/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")