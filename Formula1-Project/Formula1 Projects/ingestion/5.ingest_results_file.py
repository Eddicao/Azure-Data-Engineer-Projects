# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the Spark Dataframe Reader

# COMMAND ----------

# MAGIC %md
# MAGIC - display(dbutils.fs.mounts())
# MAGIC - %fs
# MAGIC - ls /mnt/formula1dl1123/raw

# COMMAND ----------

###  spark.read.json("/mnt/formula1dl1123/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

# COMMAND ----------

## Specify a schema
results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("grid", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("positionText", StringType(), True),
                                       StructField("positionOrder", IntegerType(), True),
                                       StructField("points",FloatType(), True),
                                       StructField("laps", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True),
                                       StructField("fastestLap", IntegerType(), True),
                                       StructField("rank", IntegerType(), True),
                                       StructField("fastestLapTime", StringType(), True),
                                       StructField("fastestLapSpeed", FloatType(), True),
                                       StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read \
    .option("header", True) \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")


# COMMAND ----------

# type(results_df)
# results_df.show()

display(results_df)
# results_df.describe().show()
# results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - drop unwanted columns

# COMMAND ----------

results_dropped_df = results_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns & Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col, concat

# COMMAND ----------

results_final_df = results_dropped_df.withColumnRenamed("resultId", "result_id") \
                                     .withColumnRenamed("raceId", "race_id") \
                                     .withColumnRenamed("driverId", "driver_id") \
                                     .withColumnRenamed("constructorId", "constructor_id") \
                                     .withColumnRenamed("positionText", "position_text") \
                                     .withColumnRenamed("positionOrder", "position_order") \
                                     .withColumnRenamed("fastestLap", "fastest_lap") \
                                     .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                     .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                     .withColumn("ingestion_date", current_timestamp()) \
                                     .withColumn("data_source", lit(v_data_source)) \
                                     .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1
# MAGIC
# MAGIC - use mode - append to add more data into the file - in stead of overwrite data
# MAGIC - use for loop - to find & drop partition with race_id already inside
# MAGIC - use if to check the existence of results file - for first run 

# COMMAND ----------

### BE CAREFUL with COLLECT - just for small data
#for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):   ## for the first run, when table not be there
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id}) ") ## help to drop partition

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results") 
### => this will come with duplicates if run many times - in SPARK capabilities for DATA LAKE


# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")
# results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2
# MAGIC ##### use function - .insertInto() - to insert dataframe into the table - and Spark bases on the last column - partitioned column
# MAGIC ##### use this for first time - for cutover data 
# MAGIC + results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")
# MAGIC ##### then use this to add new data - new partition come along:
# MAGIC + results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# MAGIC
# MAGIC #### this method might be slightly more efficient - instead of manually go & find the partitions, Spark will do it & overwrite 
# MAGIC + ALl 02 functions are kept in common_functions / includes folder - go there to check

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# MAGIC %md ##### For Delta session - change function to upsert the new data
# MAGIC merge function: very expensive to run - be careful
# MAGIC #### => make sure - always specify the PARTITION COLUMNs - the querries better

# COMMAND ----------

merge_condition = "( tgt.result_id = upd.result_id ) AND  ( tgt.race_id = src.race_id )"
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dl1123/processed/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### after run the code with 02 small data in folder 28-03 and 18-04, now we should drop the table and start to run with the BIG CUTOVER data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_processed.results;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove DUPLICATES in data before going further with processing part
# MAGIC - can recheck the source
# MAGIC - or self remove the data duplicated
# MAGIC from 24909 to 24869 with 91 removed 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_processed.results WHERE race_id = 540 AND driver_id = 229;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Since we can no decide that which data is correct & which is wrong => do de-dup & give the de-duping to SPARK decide to keep
# MAGIC use function dropDuplicates - in 149 of Udemy

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates('race_id', 'driver_id')

# COMMAND ----------

