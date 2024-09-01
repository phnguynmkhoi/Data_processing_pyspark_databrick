# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

dbutils.widgets.text("p_file_date",'2021-03-28')

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using Spark dataframer reader

# COMMAND ----------

from pyspark.sql.types import *
qualifying_schema = StructType([
    StructField('qualifyId',StringType(),True),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('q1',StringType(),True),
    StructField('q2',StringType(),True),
    StructField('q3',StringType(),True),
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiLine',True).json(f'dbfs:/FileStore/raw/{v_file_date}/qualifying')

# COMMAND ----------

qualifying_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

final_df = qualifying_df.withColumnRenamed('raceId','race_id').\
                        withColumnRenamed('driverId','driver_id').\
                        withColumnRenamed('qualifyId','qualify_id').\
                        withColumnRenamed('constructorId','contructor_id').\
                        withColumn('ingestion_date',current_timestamp()).\
                        withColumn('file_date',lit(v_file_date))
                        

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the parquet file

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

overwrite_table(final_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


