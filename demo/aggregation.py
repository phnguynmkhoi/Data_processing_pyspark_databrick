# Databricks notebook source
race_result_df = spark.read.parquet('/FileStore/presentation/race_results')

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct, avg, max, min, sum, col

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Count number of race 

# COMMAND ----------

race_result_df.select(countDistinct('race_name').alias('total_races')).show()

# COMMAND ----------

# MAGIC %md ##### Total points of Lewis Hamilton in 2020

# COMMAND ----------

race_result_df.where('driver_name = "Lewis Hamilton" and race_year=2020').select(sum('points').alias('total points')).show()

# COMMAND ----------

# MAGIC %md ##### Driver with total points > 1000 from 2015 to now and number of races they participated in which total points is greater than 1000

# COMMAND ----------

race_result_df.where('race_year>=2015').groupBy('driver_name').agg(sum('points').alias('total_points'), count('race_name').alias('total_races')).orderBy(col('total_points').desc()).where("total_points > 1000").show()

# COMMAND ----------

# MAGIC %md ##### Total points of each driver between each year(2019,2020)

# COMMAND ----------

groupBy_df = race_result_df.where('race_year>=2019 and race_year<=2020').groupBy('race_year','driver_name').agg(sum('points').alias('total_points'), count('race_name').alias('total_races')).orderBy(col('race_year'),col('total_points').desc())
groupBy_df.show()

# COMMAND ----------

# MAGIC %md ##### Total points of each driver between each year with rank(Window function)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc 

driverRankSpec = Window.partitionBy('race_year').orderBy(col('total_points').desc())
groupBy_df.withColumn('rank', rank().over(driverRankSpec)).show(100)

# COMMAND ----------


