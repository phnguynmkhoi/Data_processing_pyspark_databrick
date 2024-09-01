# Databricks notebook source
p_file_date = '2021-04-18'
dbutils.notebook.run('ingest_drivers_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('ingest_constructors_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('ingest_laptimes_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('ingest_pitstops_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('ingest_qualifying_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('ingest_results_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('ingest_circuit_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('ingest_race_file',0,{'p_file_date':p_file_date})

# COMMAND ----------


