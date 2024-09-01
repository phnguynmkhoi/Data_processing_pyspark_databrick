# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the dataframe using the spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

raw_contructor_df = spark.read.\
         json(f'/FileStore/raw/{v_file_date}/constructors.json',schema=constructor_schema)

# COMMAND ----------

raw_contructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 -Drop the unwanted column from the dataframe

# COMMAND ----------

drop_contructor_df = raw_contructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Renamed columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
final_constructor_df = drop_contructor_df.withColumnRenamed('constructorId','constructor_id').\
                                          withColumnRenamed('constructorRef','constructor_ref').\
                                          withColumn('ingestion_date',current_timestamp()).\
                                          withColumn('file_date',lit(v_file_date))

# COMMAND ----------

final_constructor_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

final_constructor_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit("success")
