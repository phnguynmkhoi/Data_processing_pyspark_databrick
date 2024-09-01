# Databricks notebook source
races_df = spark.read.parquet('/FileStore/processed/races')

# COMMAND ----------

from pyspark.sql.functions import col
filtered_race_df = races_df.filter(col('race_year')==2019)
filtered_race_df.show(5)

# COMMAND ----------


