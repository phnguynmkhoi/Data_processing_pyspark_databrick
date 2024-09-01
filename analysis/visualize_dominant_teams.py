# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1 - Driver dominant list each year and their ranking

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW v_dominant_driver;
# MAGIC CREATE TEMP VIEW v_dominant_driver
# MAGIC AS
# MAGIC SELECT race_year, 
# MAGIC       driver_name,
# MAGIC       count(1) as total_races,
# MAGIC       sum(calculated_points) as total_points,
# MAGIC       avg(calculated_points) as avg_points,
# MAGIC       rank() OVER (
# MAGIC         PARTITION BY race_year
# MAGIC         ORDER BY avg(calculated_points) DESC
# MAGIC       ) as year_rank
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY race_year, driver_name
# MAGIC ORDER BY race_year DESC, year_rank ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2 - Top 10 ranking drivers each year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     driver_name,
# MAGIC     COUNT(1) AS total_races,
# MAGIC     SUM(calculated_points) AS total_points,
# MAGIC     AVG(calculated_points) AS avg_points
# MAGIC FROM
# MAGIC     f1_presentation.calculated_race_results f
# MAGIC WHERE
# MAGIC     driver_name IN (SELECT driver_name FROM v_dominant_driver v WHERE v.year_rank <= 10 and f.race_year=v.race_year)
# MAGIC GROUP BY
# MAGIC     race_year,
# MAGIC     driver_name
# MAGIC ORDER BY
# MAGIC     race_year DESC,
# MAGIC     avg_points DESC

# COMMAND ----------


