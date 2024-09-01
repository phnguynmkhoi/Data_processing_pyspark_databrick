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

from pyspark.sql.functions import col,lit
race_result_df = spark.read.parquet("/FileStore/presentation/race_results")\
                 .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,count,when
constructor_standing_df = race_result_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))\
                         .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
teamRankSpec = Window.partitionBy("race_year").orderBy(col("total_points").desc(),col("wins").desc())
constructor_standing_df = constructor_standing_df.withColumn("rank",rank().over(teamRankSpec))

# COMMAND ----------

constructor_standing_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standing")

# COMMAND ----------


