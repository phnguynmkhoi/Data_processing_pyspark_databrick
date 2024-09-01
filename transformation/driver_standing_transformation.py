# Databricks notebook source
dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

race_results_year_list = spark.read.parquet("/FileStore/presentation/race_results")\
                        .filter(f"file_date = '{v_file_date}'")\
                        .select('race_year')\
                        .distinct()\
                        .collect()
# print(race_results_year_list/)
race_year_list = [x['race_year'] for x in race_results_year_list]

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet("/FileStore/presentation/race_results")\
                  .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

# MAGIC %md ##### Create a driver standing for each year

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,count,lit

driver_standing_df = race_results_df.groupBy('race_year','driver_name', 'driver_nationality', 'team')\
                                    .agg(sum('points').alias('total_points'), count(when(col('position') == 1,True)).alias('wins'))\
                                    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md ##### Add rank 

# COMMAND ----------

from pyspark.sql.functions import dense_rank,desc,asc
from pyspark.sql.window import Window

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
ranking_df = driver_standing_df.withColumn('rank', dense_rank().over(driverRankSpec))

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

ranking_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_rankings')
# overwrite_table(ranking_df,'f1_presentation','driver_rankings','race_year')

# COMMAND ----------


