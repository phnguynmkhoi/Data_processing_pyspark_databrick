# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest race files

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Define the schema

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([StructField('raceId',IntegerType(),True),
                        StructField('year',IntegerType(),True),
                        StructField('round',IntegerType(),True),
                        StructField('circuitId',IntegerType(),True),
                        StructField('name',StringType(),True),
                        StructField('date',StringType(),True),
                        StructField('time',StringType(),True),
                        StructField('url',StringType(),True),
                        ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Read the data from races.csv file

# COMMAND ----------

raw_df = spark.read.csv(f'/FileStore/raw/{v_file_date}/races.csv', header=True, schema=schema)

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename column

# COMMAND ----------

from pyspark.sql.functions import lit
renamed_df = raw_df.withColumnRenamed('raceId', 'race_id').\
                    withColumnRenamed('year','race_year').\
                    withColumnRenamed('circuitId','circuit_id').\
                    withColumn('file_date',lit(v_file_date))

# COMMAND ----------

renamed_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Transform data

# COMMAND ----------

from pyspark.sql.functions import col,lit,to_timestamp,concat,current_timestamp
transformed_df = renamed_df.\
                 withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')).\
                 withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

transformed_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Select the final data and load it to processed folder

# COMMAND ----------

final_df = transformed_df.drop('date','time','url')

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("success")
