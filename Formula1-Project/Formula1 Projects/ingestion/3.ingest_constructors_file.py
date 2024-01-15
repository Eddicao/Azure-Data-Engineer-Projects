# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

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

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

## Specify a schema
constructors_schema = "constructorId INT,constructorRef STRING, name STRING, nationality STRING,url STRING"

# COMMAND ----------

constructors_df = spark.read \
    .option("header", True) \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")


# COMMAND ----------

# type(constructors_df)
# constructors_df.show()

display(constructors_df)
# constructors_df.describe().show()
# constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - drop unwanted columns

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns & Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                .withColumnRenamed("constructorRef", "constructor_ref") \
                                                .withColumn("ingestion_date", current_timestamp()) \
                                                .withColumn("data_source", lit(v_data_source)) \
                                                .withColumn("file_date", lit(v_file_date))    
                                  

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

### for Python create tables in Processed  Container ADLS
### constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
# constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")  ### for delta sesion

# COMMAND ----------

## display(spark.read.parquet("/mnt/formula1dl1123/processed/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")