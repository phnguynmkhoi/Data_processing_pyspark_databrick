# Databricks notebook source
# MAGIC %md
# MAGIC #### Read the result file

# COMMAND ----------

dbutils.widgets.text("p_file_date",'2021-03-28')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Ingest the json file

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("resultId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", DoubleType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

raw_result_df = spark.read.json(f'/FileStore/raw/{v_file_date}/results.json', schema=schema)

# COMMAND ----------

raw_result_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Renamed and drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import lit
transformed_result_df = raw_result_df.withColumnsRenamed({
    'resultId':'result_id',
    'raceId':'race_id',
    'driverId':'driver_id',
    'constructorId':'constructor_id',
    'fastestLapTime':'fastest_lap_time',
    'fastestLap':'fastest_lap',
    'fastestLapSpeed':'fastest_lap_speed',
    'positionOrder':'position_order',
    'positionText':'position_text'
}).\
    withColumn('file_date',lit(v_file_date)).\
    drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Adding ingestion date columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_result_df = transformed_result_df.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

final_result_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the dataframe in parquet format in processed folder with partitioned by race_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Incremental load - Method 1

# COMMAND ----------

# for race_id_collect in final_result_df.select('race_id').distinct().collect():
#     if spark._jsparkSession.catalog().tableExists('f1_processed.results'):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_collect.race_id})")
# final_result_df.write.partitionBy('race_id').mode('append').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

overwrite_table(final_result_df,'f1_processed','results','race_id')

# COMMAND ----------

dbutils.notebook.exit("success")
