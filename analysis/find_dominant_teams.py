# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1 - Dominant team list of all times which have more than 50 races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC       count(1) as total_races,
# MAGIC       sum(calculated_points) as total_points,
# MAGIC       avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY team_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2 - Dominant teams list of 2011-2020 which have more than 50 races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC       count(1) as total_races,
# MAGIC       sum(calculated_points) as total_points,
# MAGIC       avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2011 AND 2020
# MAGIC GROUP BY team_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2 - Dominant teams list of 2001-2010 which have more than 50 races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC       count(1) as total_races,
# MAGIC       sum(calculated_points) as total_points,
# MAGIC       avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2001 AND 2010
# MAGIC GROUP BY team_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------


