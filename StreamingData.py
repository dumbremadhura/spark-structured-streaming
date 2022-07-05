# Databricks notebook source
from pyspark.sql.types import (
  StructType,
  StructField,
  IntegerType,
  StringType,
  FloatType
)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/datasets")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/datasets")

# COMMAND ----------

schema = StructType([StructField('Id', IntegerType(), False),
                     StructField('Company', StringType(), False),
                     StructField('Product', StringType(), False),
                     StructField('TypeName', StringType(), False),
                     StructField('Price_euros', FloatType(), False)])

# COMMAND ----------

laptop_data = spark.read.format('csv')\
                        .option('header', True)\
                        .schema(schema)\
                        .load("dbfs:/FileStore/datasets/laptops.csv")
laptop_data.display()

# COMMAND ----------

# MAGIC %md Check if it is a streaming dataframe

# COMMAND ----------

print("streaming data or not : ", laptop_data.isStreaming)

# COMMAND ----------

# MAGIC %md How to create a streaming dataframe?

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/datasets/laptop_source_stream") #empty directory

# COMMAND ----------

laptop_stream_data = spark.readStream\
                          .format('csv')\
                          .option('header', True)\
                          .schema(schema)\
                          .load('dbfs:/FileStore/datasets/laptop_source_stream')
print("streaming dataframe or not? ", laptop_stream_data.isStreaming)

# COMMAND ----------

# MAGIC %md Check if the data was picked up by the continous readStream , after source data was added.

# COMMAND ----------

laptop_stream_data.display()

# COMMAND ----------

# MAGIC %md Transform a column to convert currency in streaming data

# COMMAND ----------

from pyspark.sql.functions import col,round
laptop_stream_data_updated = laptop_stream_data.withColumn('Price_usd' , round(col('Price_euros')* 1.4389, 2))
laptop_stream_data_updated.display()

# COMMAND ----------

# MAGIC %md select few columns out of the data

# COMMAND ----------

laptop_stream_data_updated.select('Price_usd', 'Typename').display()

# COMMAND ----------

premium_laptops = laptop_stream_data_updated.select('Id', 'Company' , 'Price_usd').where('Price_usd > 2000')
premium_laptops.display()

# COMMAND ----------

# MAGIC %md **Demo on Triggers**

# COMMAND ----------

# MAGIC %md **1. Fixed interval micro batch trigger**

# COMMAND ----------

premium_laptops.writeStream\
                .format('memory')\
                .queryName('premium_laptops_20')\
                .trigger(processingTime='20 seconds')\
                .start()

# COMMAND ----------

# MAGIC %md Query the in-memory table : #note : this is a batch query and will not be continously updated

# COMMAND ----------

spark.sql('select company, avg(price_usd) from premium_laptops_20 group by company order by company').display()

# COMMAND ----------

# MAGIC %md **2. One-Time micro batch trigger**

# COMMAND ----------

premium_laptops.writeStream\
                .format('memory')\
                .queryName('premium_laptops_once')\
                .trigger(once=True)\
                .start()

# COMMAND ----------

# MAGIC %md Perform Aggregation on the stream data in-memory table

# COMMAND ----------

spark.sql('select company, avg(price_usd) from premium_laptops_once group by company order by company').display()
