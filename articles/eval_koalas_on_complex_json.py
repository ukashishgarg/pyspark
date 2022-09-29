# Databricks notebook source
# MAGIC %md
# MAGIC # Test Delta file read operation that contains the complex json/nested structure
# MAGIC ***
# MAGIC 
# MAGIC This test is with Koalas and spark dataframe to understand the difference between the two. 

# COMMAND ----------

import databricks.koalas as ks
import pyspark.pandas as ps
import pandas as pd
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data with complex json nested structure 
# MAGIC ***

# COMMAND ----------

# create payloads 
payload_data1 = {"EmpId": "A01", "IsPermanent": True, "Department": [{"DepartmentID": "D1", "DepartmentName": "Data Science"}]}
payload_data2 = {"EmpId": "A02", "IsPermanent": False, "Department": [{"DepartmentID": "D2", "DepartmentName": "Application"}]}
payload_data3 = {"EmpId": "A03", "IsPermanent": True, "Department": [{"DepartmentID": "D1", "DepartmentName": "Data Science"}]}
payload_data4 = {"EmpId": "A04", "IsPermanent": False, "Department": [{"DepartmentID": "D2", "DepartmentName": "Application"}]}

# create data structure
data =[
  {"BranchId": 1, "Payload": payload_data1},
  {"BranchId": 2, "Payload": payload_data2},
  {"BranchId": 3, "Payload": payload_data3},
  {"BranchId": 4, "Payload": payload_data4}
]

# dump data to json 
jsonData = json.dumps(data)

# append json data to list 
jsonDataList = []
jsonDataList.append(jsonData)

# parallelize json data
jsonRDD = sc.parallelize(jsonDataList)

# store data to spark dataframe 
df = spark.read.json(jsonRDD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store created test data as the Delta file
# MAGIC ***

# COMMAND ----------

# Select the path where you wanna store your delta file
table_name = "/testautomation/EmployeeTbl"

# Perform write operation
(df.write
  .mode("overwrite")
  .format("delta")
  .option("overwriteSchema", "true")
  .save(table_name))

# List the file created inside testautomation folder
dbutils.fs.ls("./testautomation/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the delta file
# MAGIC ***

# COMMAND ----------

dbutils.fs.ls("./testautomation/EmployeeTbl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Delta file using Spark API and store as Spark Dataframe
# MAGIC ***

# COMMAND ----------

df = spark.read.load(table_name)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Delta file using koalas/pandas API and store as Spark Dataframe
# MAGIC ***

# COMMAND ----------

kldf = ps.read_delta(table_name)
kldf

# COMMAND ----------

kldf.head()

# COMMAND ----------

display(kldf)
