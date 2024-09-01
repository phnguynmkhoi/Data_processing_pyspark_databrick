# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1 - Dominant drivers list of all times who have been raced more than 100 races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name,
# MAGIC       count(1) as total_races,
# MAGIC       sum(calculated_points) as total_points,
# MAGIC       avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING total_races >= 100
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2 - Dominant drivers list of 2011-2020 who have been raced more than 50 races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name,
# MAGIC       count(1) as total_races,
# MAGIC       sum(calculated_points) as total_points,
# MAGIC       avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2011 AND 2020
# MAGIC GROUP BY driver_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2 - Dominant drivers list of 2001-2010 who have been raced more than 50 races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name,
# MAGIC       count(1) as total_races,
# MAGIC       sum(calculated_points) as total_points,
# MAGIC       avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2001 AND 2010
# MAGIC GROUP BY driver_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------


