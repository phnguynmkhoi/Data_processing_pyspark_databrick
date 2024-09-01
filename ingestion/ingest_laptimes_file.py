# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_file_date",'2021-03-28')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read csv file using Spark dataframer reader

# COMMAND ----------

from pyspark.sql.types import *
lap_times_schema = StructType([
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),True),
    StructField('lap',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('miliseconds',IntegerType(),True),
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f'dbfs:/FileStore/raw/{v_file_date}/lap_times')

# COMMAND ----------

lap_times_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

final_df = lap_times_df.withColumnRenamed('raceId','race_id').\
                        withColumnRenamed('driverId','driver_id').\
                        withColumn('ingestion_date',current_timestamp()).\
                        withColumn('file_date',lit(v_file_date))
                        

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the parquet file

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

overwrite_table(final_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


