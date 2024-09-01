# Databricks notebook source
def rearrange_partition_column(df, partition_column):
    print(df)
    df_schema = df.schema.names
    if partition_column in df_schema:
        df_schema.remove(partition_column)
        df_schema.append(partition_column)
    return df.select(df_schema)

def overwrite_table(df, db_name, table_name, partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df = rearrange_partition_column(df, partition_column)
    print(df)
    if spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}'):
        df.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
    else:
        df.write.partitionBy(f'{partition_column}').mode('overwrite').format('parquet').saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------


