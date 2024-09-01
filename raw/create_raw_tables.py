# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest csv file

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1 - Ingest circuits file

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits (
# MAGIC   circuitId INT,
# MAGIC   circuitRef STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   lat FLOAT,
# MAGIC   lng FLOAT,
# MAGIC   alt INT,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING csv 
# MAGIC OPTIONS (path "/FileStore/raw/circuits.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2 - Ingest races file

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE If EXISTS f1_raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races (
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date DATE,
# MAGIC   time STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/raw/races.csv", headers true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.races;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest json file

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1 - Create constructors table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors (
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path "/FileStore/raw/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.constructors

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2 - Create drivers table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename: STRING, surname: STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path "/FileStore/raw/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3 - Create results table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results (
# MAGIC     resultId INT,
# MAGIC     raceId INT,
# MAGIC     driverId INT,
# MAGIC     constructorId INT,
# MAGIC     number INT,
# MAGIC     grid INT,
# MAGIC     position INT,
# MAGIC     positionText STRING,
# MAGIC     positionOrder INT,
# MAGIC     points INT,
# MAGIC     laps INT,
# MAGIC     time STRING,
# MAGIC     milliseconds INT,
# MAGIC     fastestLap INT,
# MAGIC     rank INT,
# MAGIC     fastestLapTime STRING,
# MAGIC     fastestLapSpeed FLOAT,
# MAGIC     statusId STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path "/FileStore/raw/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw                                  .results;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 4 - Create pit_stops table(multiple lines)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
# MAGIC     driverId INT,
# MAGIC     duration STRING,
# MAGIC     lap INT,
# MAGIC     milliseconds INT,
# MAGIC     raceId INT,
# MAGIC     stop INT,
# MAGIC     time STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS(path "/FileStore/raw/pit_stops.json", multiline true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.pit_stops

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5 - Create lap_times table(multiple files)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
# MAGIC     raceId INT,
# MAGIC     driverId INT,
# MAGIC     lap INT,
# MAGIC     position INT,
# MAGIC     time STRING,
# MAGIC     milliseconds INT
# MAGIC ) 
# MAGIC USING CSV
# MAGIC OPTIONS (path "/FileStore/raw/lap_times");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.lap_times;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 6 - Create qualifying table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
# MAGIC     constructorId INT,
# MAGIC     driverId INT,
# MAGIC     number INT,
# MAGIC     position INT,
# MAGIC     q1 STRING,
# MAGIC     q2 STRING,
# MAGIC     q3 STRING,
# MAGIC     qualifyId INT,
# MAGIC     raceId INT
# MAGIC ) 
# MAGIC USING json
# MAGIC OPTIONS (path "/FileStore/raw/qualifying", multiLine true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.qualifying;

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED f1_raw.qualifying;

# COMMAND ----------


