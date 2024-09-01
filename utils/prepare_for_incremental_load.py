# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION '/FileStore/processed'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC LOCATION '/FileStore/presentation'
