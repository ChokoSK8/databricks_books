# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/antoine.b@datascientest.com/books.json")

# COMMAND ----------

df_dev = df.limit(5)

# COMMAND ----------

df_dev.show()

# COMMAND ----------

df_dev.dtypes

# COMMAND ----------

df_dev_2 = df_dev.withColumn("publishedDate", col("publishedDate.$date"))

# COMMAND ----------

df_dev_2.dtypes

# COMMAND ----------

df_dev_2.select(col("publishedDate").cast("timestamp")).show()

# COMMAND ----------

df_dev_3 = df_dev.withColumn("publishedDate", col("publishedDate.$date").cast("timestamp"))

# COMMAND ----------

df_dev_3.dtypes
