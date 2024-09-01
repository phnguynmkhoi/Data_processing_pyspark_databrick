# Databricks notebook source
circuit_df = spark.read.parquet('/FileStore/processed/circuit')
races_df = spark.read.parquet('/FileStore/processed/races').filter('race_year = 2019')

# COMMAND ----------

race_join_circuit_df = circuit_df.join(races_df, races_df.circuit_id==circuit_df.circuit_id, how='inner')

# COMMAND ----------

race_join_circuit_df = race_join_circuit_df.drop(circuit_df.name)
