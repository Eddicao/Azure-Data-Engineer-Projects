# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

## Specify a schema
name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                       StructField("driverRef", StringType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("code", StringType(), True),
                                       StructField("name", name_schema),
                                       StructField("dob", DateType(), True),
                                       StructField("nationality", StringType(), True),
                                       StructField("url", StringType(), True)
])
                                    

# COMMAND ----------

drivers_df = spark.read \
    .option("header", True) \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")


# COMMAND ----------

# type(drivers_df)
# drivers_df.show()
display(drivers_df)
# drivers_df.describe().show()
drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns & Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col, concat

# COMMAND ----------

drivers_added_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                             .withColumnRenamed("driverRef", "driver_ref") \
                             .withColumn("ingestion_date", current_timestamp()) \
                             .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                             .withColumn("data_source", lit(v_data_source)) \
                             .withColumn("file_date", lit(v_file_date))
                                  

# COMMAND ----------

display(drivers_added_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_added_df.drop('url')

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

### drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
## drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")  ### For Delta session

# COMMAND ----------

## display(spark.read.parquet("/mnt/formula1dl1123/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")