-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####  Create new DATABASE to keep all raw tables in a separate database

-- COMMAND ----------

----- CREATE DATABASE f1_raw;
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Create circuits table - CSV files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;  --- due to the header code
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dl1123/raw/circuits.csv", header true)

---- external tables - use DDL schema

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Create races table  - CSV file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "/mnt/formula1dl1123/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;  --- no header inside the file
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formula1dl1123/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers; --- no header inside the file
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,  ----   be careful with this complex structure 
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/formula1dl1123/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md ##### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results; --- no header inside the file
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/formula1dl1123/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops; --- no header inside the file but with MULTILINEs
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/formula1dl1123/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Create tables for list of files / FOLDER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times; --- no header inside the file // not care about the header
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dl1123/raw/lap_times")

---- this will pick up the files in the folder

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying; --- no header inside the file but with MULTILINEs
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/formula1dl1123/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;

-- COMMAND ----------

Show tables in f1_raw;

-- COMMAND ----------

