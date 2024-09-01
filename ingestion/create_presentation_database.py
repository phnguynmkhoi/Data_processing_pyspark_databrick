# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION "/FileStore/processed"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE f1_processed

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/processed", recurse=True)

# COMMAND ----------


