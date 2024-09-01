# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the data from circuits.csv

# COMMAND ----------

# MAGIC %md Define schema for faster data ingestion

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([StructField('circuitId', IntegerType(), True), 
          StructField('circuitRef', StringType(), True), 
          StructField('name', StringType(), True),
          StructField('location', StringType(), True),
          StructField('country', StringType(), True),
          StructField('lat', DoubleType(), True),
          StructField('lng',DoubleType(),True),
          StructField('alt', IntegerType(), True),
          StructField('url', StringType(), True)])

# COMMAND ----------

circuit_df = spark.read.csv(f'/FileStore/raw/{v_file_date}/circuits.csv', header=True, schema=schema)

# COMMAND ----------

circuit_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select the required columns

# COMMAND ----------

from pyspark.sql.functions import col
circuit_selected_df = circuit_df.select(col('circuitId'),
                                        col('circuitRef'),
                                        col('name'),
                                        col('location'),
                                        col('country'),
                                        col('lat'),
                                        col('lng'),
                                        col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit
circuit_renamed_df = circuit_selected_df.withColumnRenamed('circuitId','circuit_id').\
                                        withColumnRenamed('circuitRef','circuit_ref').\
                                        withColumnRenamed('lat','latitude').\
                                        withColumnRenamed('lng','longitude').\
                                        withColumnRenamed('alt','altitude').\
                                        withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
circuit_final_df = circuit_renamed_df.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data as parquet

# COMMAND ----------

circuit_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


