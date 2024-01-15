# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

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

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

#dbutils.widgets.help()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

## Specify a schema
circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                       StructField("circuitRef", StringType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("location", StringType(), True),
                                       StructField("country", StringType(), True),
                                       StructField("lat", DoubleType(), True),
                                       StructField("lng", DoubleType(), True),
                                       StructField("alt", DoubleType(), True),
                                       StructField("url", StringType(), True)

])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
### this code cost 02 spark jobs - to slow - bad performance
### need to define the schema first for better performance

# COMMAND ----------

# type(circuits_df)
# circuits_df.printSchema()
# circuits_df.show()
display(circuits_df)
# circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                        .withColumnRenamed("circuitRef", "circuit_ref") \
                        .withColumnRenamed("lat", "latitude") \
                        .withColumnRenamed("lng", "longtitude") \
                        .withColumnRenamed("alt", "altitude") \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))
                                  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# display(circuits_selected_df)
# display(circuits_renamed_df)
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

## circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")     ### for Python create tables in Processed  Container ADLS
#  circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")
## create managed tables in database f1_processed

# COMMAND ----------

#  display(spark.read.parquet("/mnt/formula1dl1123/processed/circuits"))  ### this code for parquet file, in Delta session, it get error

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")