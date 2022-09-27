# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate Spark, Pandas, & Koalas dataframe 
# MAGIC ***

# COMMAND ----------

spark.version

# COMMAND ----------

import databricks.koalas as ks
import pandas as pd
import datetime
from random import choice, randint,randrange
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType, StructField, StructType, DecimalType

# COMMAND ----------

rand_range = 0.9003948570
schema = StructType(
    [
        StructField("number", IntegerType(), nullable=False),
        StructField("random_number", IntegerType(), nullable=True),
    ]
)
data = list()

# Create the score column data
max_5million = 5000000
for i in range(0, max_5million):
    data.append(
        {
            "number": randint(10, 10000000),
        }
    )

# Create the Spark dataframe
df = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark).createDataFrame(data, schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# Generate the randomized score column
df = df.withColumn("random_number", F.col("number") + (choice([-1, 1]) * (1 - rand_range)))
display(df)

# COMMAND ----------

df.select("random_number").dtypes

# COMMAND ----------

df1 = df.withColumn("random_number",df["random_number"].cast(DecimalType(6,4)))
df1.select("random_number").dtypes

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Spark Dataframe count performance
# MAGIC ***

# COMMAND ----------

start = datetime.datetime.now()
df.count()
end = datetime.datetime.now()
delta = end - start
# print(delta)
print(int(delta.total_seconds() * 1000)) # milliseconds

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK DATAFRAME PERFORMANCE (5 million records):
# MAGIC 1. First Run:  2372 ms
# MAGIC 2. Second Run: 1298 ms
# MAGIC 3. Third Run:  1127 ms

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Pandas Dataframe performance
# MAGIC ***

# COMMAND ----------

start = datetime.datetime.now()
pdf = df.toPandas()
end = datetime.datetime.now()
delta = end - start
# print(delta)
print(int(delta.total_seconds() * 1000)) # milliseconds

# COMMAND ----------

# MAGIC %md
# MAGIC Spark to Pandas conversion performance (5 million records):
# MAGIC 1. First Run: 2418 ms
# MAGIC 2. Second Run: 1486 ms
# MAGIC 3. Third Run:  1488 ms

# COMMAND ----------

start = datetime.datetime.now()
pdf.count()
end = datetime.datetime.now()
delta = end - start
print(int(delta.total_seconds() * 1000)) # milliseconds

# COMMAND ----------

# MAGIC %md
# MAGIC Pandas count performance (5 million records):
# MAGIC 1. First Run: 24 ms
# MAGIC 2. Second Run: 23 ms
# MAGIC 3. Third Run:  23 ms

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Koalas Dataframe performance
# MAGIC ***

# COMMAND ----------

start = datetime.datetime.now()
ksdf = df.to_koalas()
end = datetime.datetime.now()
delta = end - start
# print(delta)
print(int(delta.total_seconds() * 1000)) # milliseconds

# COMMAND ----------

# MAGIC %md
# MAGIC Spark to Koalas conversion performance (5 million records):
# MAGIC 1. First Run: 130 ms
# MAGIC 2. Second Run: 75 ms
# MAGIC 3. Third Run:  78 ms

# COMMAND ----------

start = datetime.datetime.now()
ksdf.count()
end = datetime.datetime.now()
delta = end - start
print(int(delta.total_seconds() * 1000)) # milliseconds

# COMMAND ----------

# MAGIC %md
# MAGIC Koalas count performance (5 million records):
# MAGIC 1. First Run: 1650 ms
# MAGIC 2. Second Run: 1484 ms
# MAGIC 3. Third Run:  1503 ms
