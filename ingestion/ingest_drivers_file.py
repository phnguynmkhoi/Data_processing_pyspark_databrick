# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest the drivers.json file

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *

name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

schema = StructType([
    StructField('driverId', IntegerType(), True),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema, True),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
    ])

# COMMAND ----------

raw_driver_df = spark.read.json(f'/FileStore/raw/{v_file_date}/drivers.json',schema=schema)

# COMMAND ----------

raw_driver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add new column

# COMMAND ----------

from pyspark.sql.functions import lit,col,concat,current_timestamp

# COMMAND ----------

transformed_df = raw_driver_df.withColumnRenamed('driverId', 'driver_id').\
                               withColumnRenamed('driverRef', 'driver_ref').\
                               withColumn('ingestion_date',current_timestamp()).\
                               withColumn('name', concat(col('name.forename'),lit(' '),col('name.surname'))).\
                               withColumn('file_date',lit(v_file_date))

# COMMAND ----------

transformed_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns

# COMMAND ----------

final_drivers_df = transformed_df.drop('url')

# COMMAND ----------

final_drivers_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the parquet file

# COMMAND ----------

final_drivers_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


