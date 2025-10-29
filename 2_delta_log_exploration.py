# Databricks notebook source
# MAGIC %md
# MAGIC # **DELTA LOG**

# COMMAND ----------

data = [(1,100,'aa'),(2,200,'bb'),(3,300,'cc'),(4,400,'dd')]

df = spark.createDataFrame(data, ['id','salary','name'])

display(df)

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .save("/Volumes/deltalakeansh/default/deltavol/demosink/")

# COMMAND ----------

df = spark.read.format("json")\
            .load("/Volumes/deltalakeansh/default/deltavol/demosink/_delta_log/00000000000000000001.json")
display(df)

# COMMAND ----------

