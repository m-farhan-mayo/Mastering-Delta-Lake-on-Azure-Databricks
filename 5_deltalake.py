# Databricks notebook source
# MAGIC %md
# MAGIC # **DELTA LOG LEVEL SCHEMA CHANGES**

# COMMAND ----------

data = [(1,100,'xyz',1),(9,200,'bb',1),(10,300,'cc',1)]

df = spark.createDataFrame(data, ['cust_id','income','name','tip'])

display(df)


# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .save("/Volumes/deltalakeansh/default/deltavol/schemalevel/")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable column mapping on the Delta table
# MAGIC ALTER TABLE delta.`/Volumes/deltalakeansh/default/deltavol/schemalevel/`
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC
# MAGIC -- Rename the column
# MAGIC ALTER TABLE delta.`/Volumes/deltalakeansh/default/deltavol/schemalevel/`
# MAGIC RENAME COLUMN name TO customer_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/deltalakeansh/default/deltavol/schemalevel/`

# COMMAND ----------

df = spark.read.format("json")\
            .load("/Volumes/deltalakeansh/default/deltavol/schemalevel/_delta_log/00000000000000000002.json")
df.display()

# COMMAND ----------

