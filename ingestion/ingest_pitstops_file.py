# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pitstops.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date",'2021-03-28')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using Spark dataframer reader

# COMMAND ----------

from pyspark.sql.types import *
pit_stops_schema = StructType([
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),True),
    StructField('stop',StringType(),True),
    StructField('lap',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('duration',StringType(),True),
    StructField('miliseconds',IntegerType(),True),
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option('muitiLine',True).json(f'/FileStore/raw/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

final_df = pit_stops_df.withColumnRenamed('raceId','race_id').\
                        withColumnRenamed('driverId','driver_id').\
                        withColumn('ingestion_date',current_timestamp()).\
                        withColumn('file_date',lit(v_file_date))
                        

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the parquet file

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

overwrite_table(final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


