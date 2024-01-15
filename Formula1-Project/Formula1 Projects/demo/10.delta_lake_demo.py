# Databricks notebook source
# MAGIC %md
# MAGIC ### Objectives lessons
# MAGIC ### A - Read & write
# MAGIC 1. write data to delta lake (managed table) or (external table)
# MAGIC 1. read data from delta lake (Table) or (File)

# COMMAND ----------

dbutils.fs.mounts()
%fs
ls '/mnt/formula1dl1123/demo'

# COMMAND ----------

# display(dbutils.fs.mounts())
display(dbutils.fs.ls("/mnt/formula1dl1123/demo"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC location "/mnt/formula1dl1123/demo"
# MAGIC
# MAGIC --- we specify the location as we want all the managed tables to be created in that location

# COMMAND ----------

results_df = spark.read.option("inferSchema", True) \
                .json('/mnt/formula1dl1123/raw/2021-03-28/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC results.json => dataframe - results_df => managed table - results_managed
# MAGIC
# MAGIC ## write data to Delta Lake - managed table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed
# MAGIC
# MAGIC --- with this managed table, can use SQL to query data directly

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write data to a file location - external table

# COMMAND ----------

# results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dl1123/demo/results_external")

## this will create an external table, but to read the data, we need to create table - then use SELECT query (can not use SQL query data directly)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dl1123/demo/results_external'
# MAGIC
# MAGIC --- then we can use SELECT To read the data
# MAGIC SELECT  * FROM f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC ##### if we dont want to create a table - just read the data DIRECTLY
# MAGIC

# COMMAND ----------

results_external_df = spark.read.format('delta').load("/mnt/formula1dl1123/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### the way to create partition table or write data to partition folder

# COMMAND ----------

results_df.write.format("delta").mode("overwrite") \
    .partitionBy("constructorId") \
    .saveAsTable("f1_demo.results_partitioned")

### results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ### B - Update & Deletes
# MAGIC 1. Update data to delta lake 
# MAGIC 1. Delete data to delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC   WHERE position <= 10
# MAGIC
# MAGIC --- we do update here with SQL queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# from delta.tables import *
# from pyspark.sql.functions import *

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl1123/demo/results_managed")
# deltaTable.update("eventType = 'click'", { "eventType": "'click'" })
deltaTable.update("position <= 10", { "points": "21 - position" })

### we do update with Python code here


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DELETES

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;
# MAGIC
# MAGIC --- do delete in SQL queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl1123/demo/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ### C - Merges & Upsert
# MAGIC 1. Update & Insert data using merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upsert using Merge
# MAGIC **Merge**
# MAGIC - insert any new records (received)
# MAGIC - update any existing records for which new data (received)
# MAGIC - with a delete request, can apply that delete as well
# MAGIC => do all in one statement

# COMMAND ----------

# MAGIC %md
# MAGIC ###### for DAY 1

# COMMAND ----------

### FOR DAY 1 with driver_id [1-10]
drivers_day1_df = spark.read \
                .option("inferSchema", True) \
                .json("/mnt/formula1dl1123/raw/2021-03-28/drivers.json") \
                .filter("driverId <= 10") \
                .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### for DAY 2

# COMMAND ----------

### ### FOR DAY 2 with driver_id [6-15]
from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
                    .option("inferSchema", True) \
                    .json("/mnt/formula1dl1123/raw/2021-03-28/drivers.json") \
                    .filter("driverId BETWEEN 6 AND 15") \
                    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
### 

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### for DAY 3

# COMMAND ----------

### ### FOR DAY 3 with driver_id [1-5 & 16-20]
from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
                    .option("inferSchema", True) \
                    .json("/mnt/formula1dl1123/raw/2021-03-28/drivers.json") \
                    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
                    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### we have 02 inserts & 01 update
# MAGIC - day1 : from 1 to 10
# MAGIC - day2 : from 6 to 15
# MAGIC - day3 : 1-5 & 16-20
# MAGIC > - back to create Temp View for these - can join them using SQL statement
# MAGIC > - & NEED TO create a delta lake table IN ORDER TO RUN merge 

# COMMAND ----------

### Need to create the views to run SQL on this
drivers_day1_df.createOrReplaceTempView("drivers_day1")
drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_upsert (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC --- common to have more columns createData // updatedDate

# COMMAND ----------

# MAGIC %md
# MAGIC ##### merge using day1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_upsert tgt  ----- events need to updates
# MAGIC USING drivers_day1 upd  ----- updates
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname,createdDate ) VALUES (driverId, dob, forename, surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_upsert;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### merge using day2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_upsert tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname,createdDate ) VALUES (driverId, dob, forename, surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_upsert;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### merge using day3

# COMMAND ----------

### instead using SQL, we can use python to do UPSERT 
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl1123/demo/drivers_upsert")

deltaTable.alias("tgt").merge(drivers_day3_df.alias("upd"), "tgt.driverId = upd.driverId") \
        .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
        .whenNotMatchedInsert(values =
            {
            "driverId": "upd.driverId",
            "dob": "upd.dob",
            "forename" : "upd.forename", 
            "surname" : "upd.surname", 
            "createdDate": "current_timestamp()"
            }
        ) \
        .execute()

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_upsert;

# COMMAND ----------

# MAGIC %md
# MAGIC ### D - Powerful Facility of Delta
# MAGIC 1. History & Versioning
# MAGIC 1. Time Travel
# MAGIC 1. Vacuum

# COMMAND ----------

# MAGIC %md
# MAGIC ##### History & versioning of Delta file

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_upsert

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_upsert VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Time Travel of Delta file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_upsert TIMESTAMP AS OF '2023-11-08T12:26:48Z';  --- this is from version 1 - timestamp
# MAGIC --- get info from - DESC HISTORY f1_demo.drivers_upsert

# COMMAND ----------

# MAGIC %md - the pyspark version to see & extract the version of data

# COMMAND ----------

#### When we can get the version of data - can get the dataframe of that version
df_version1 = spark.read.format("delta").option("timestampAsOf", '2023-11-08T12:26:48Z').load("/mnt/formula1dl1123/demo/drivers_upsert")

# COMMAND ----------

display(df_version1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Vacuum of Delta file
# MAGIC - implement GDPR requirement - EU legislation in 30 days - since we have history with versions
# MAGIC - with VACUUM - usually remove history older than 7 days - can change the timeline

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_upsert

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_upsert TIMESTAMP AS OF '2023-11-08T12:27:15Z';
# MAGIC --- we can still see this data - with default - 7 days

# COMMAND ----------

# MAGIC %md - this RETAIN 0 hours => remove data immidiately - any history of the data/table will be deleted
# MAGIC ###### ALL versions deleted, but the final version - current one still be there - ONLY ONE VERSION

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 7;
# MAGIC
# MAGIC --- one version - 4 & current - 7

# COMMAND ----------

# MAGIC %md ##### The way to get back the record just deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt   --- the current version
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 4 src  --- the version before deleting records
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *   ---- insert all records -- roll back the actions

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql   SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### E - Transaction Logs
# MAGIC 1. not keep within the hive meta store - inefficient to get
# MAGIC 1. transaction log kept for 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId <= 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM  f1_demo.drivers_txn
# MAGIC WHERE driverId = 1;

# COMMAND ----------

for driver_id in range(3, 20):
  spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                SELECT * FROM f1_demo.drivers_merge
                WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ### F - Convert Parquet to Delta
# MAGIC 1. from a file system / parquet file => delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md #### convert table into delta with Python code
# MAGIC now if we just have parquet file / no table - drivers_convert_to_delta_new

# COMMAND ----------

#### from a delta table - get data frame
df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dl1123/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dl1123/demo/drivers_convert_to_delta_new`

# COMMAND ----------

