# Databricks notebook source
# MAGIC %md
# MAGIC #### Race Result Transformation

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1 - Read all the dataframe

# COMMAND ----------

races_df = spark.read.parquet('/FileStore/processed/races')
circuits_df = spark.read.parquet('/FileStore/processed/circuits')
constructors_df = spark.read.parquet('/FileStore/processed/constructors')
drivers_df = spark.read.parquet('/FileStore/processed/drivers')

# COMMAND ----------

results_df = spark.read.parquet('/FileStore/processed/results')\
             .withColumnRenamed('race_id','result_race_id')\
             .where(f'file_date = "{v_file_date}"')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Join all the dataframe

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'inner').\
                            drop(circuits_df['name']).\
                            select('race_id', 'race_year', 'name', 'race_timestamp', 'location').\
                            withColumnRenamed('race_timestamp','race_date').\
                            withColumnRenamed('name','race_name')

# COMMAND ----------

final_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id, 'inner').\
                      join(drivers_df, results_df.driver_id == drivers_df.driver_id,'inner').\
                      join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, 'inner').\
                      select('race_id','race_year', 'race_name','race_date',races_circuits_df['location'].alias('circuit_location'),
                     drivers_df.name.alias('driver_name'),drivers_df.number.alias('driver_number')
                     ,drivers_df['nationality'].alias('driver_nationality')
                     ,constructors_df.name.alias('team'),'grid','fastest_lap',results_df.time,'points',
                     'position',results_df['file_date'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add created_date `column`

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = final_df.withColumn('created_date',current_timestamp())

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

overwrite_table(final_df,'f1_presentation','race_results','race_id')
