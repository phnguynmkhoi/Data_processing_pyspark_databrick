# Databricks notebook source
race_result_df = spark.read.parquet('/FileStore/presentation/race_results')

# COMMAND ----------

race_result_df.createOrReplaceTempView('race_result')

# COMMAND ----------


