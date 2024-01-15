# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

## Specify a schema
races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("year", IntegerType(), True),
                                       StructField("round", IntegerType(), True),
                                       StructField("circuitId", IntegerType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("date", DateType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("url", StringType(), True)

])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")
### this code cost 02 spark jobs - to slow - bad performance
### need to define the schema first for better performance

# COMMAND ----------

# type(races_df)
# races_df.printSchema()
# races_df.show()
display(races_df)
# races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add ingestion date & race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, current_timestamp, lit, col, concat

# COMMAND ----------

races_added_df = races_df.withColumn("ingestion_date", current_timestamp()) \
            .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Select the columns as required

# COMMAND ----------

races_selected_df = races_added_df.select("raceID", "year", "round", "circuitId", "name", "race_timestamp", "ingestion_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Rename the columns as required

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("year", "race_year") \
                        .withColumnRenamed("circuitId", "circuit_id") \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))
                                  

# COMMAND ----------

# display(races_selected_df)
display(races_renamed_df)
# display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

### We use partition to define data into different partition
## races_renamed_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")

### for Python create tables in Processed  Container ADLS
#races_renamed_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")
## create managed tables in database f1_processed

races_renamed_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")  ### for Delta session

# COMMAND ----------

### display(spark.read.parquet("/mnt/formula1dl1123/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")