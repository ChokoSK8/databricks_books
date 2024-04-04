# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/antoine.b@datascientest.com/books.json")

# COMMAND ----------

df = df.withColumn("publishedDate", col("publishedDate.$date").cast("timestamp"))

# COMMAND ----------

df.write.json("dbfs:/FileStore/shared_uploads/antoine.b@datascientest.com/books_2.json")
