# Databricks notebook source
#Configure Variables
####################
storageName="sa27decmujahed"
containerName="container"
mountPoint="/mnt/iotdata"

configuredSource=f"wasbs://{containerName}@{storageName}.blob.core.windows.net"
ecKey=f"fs.azure.account.key.{storageName}.blob.core.windows.net"
ecValue="R5N68PlQhqUX6C/WWvbAZOyo9FwFHy4rqxJDV7LJ2kmkuksZbBYPoG0HESPy8Hnp7/me62rCGWbUgZeyDGNyew=="

# COMMAND ----------

#Create Mount Point
###################
dbutils.fs.mount(
    source=configuredSource,
    mount_point=mountPoint,
    extra_configs={ecKey:ecValue}
)

# COMMAND ----------

dbutils.fs.ls(mountPoint)

# COMMAND ----------

iotDeviceJSON="dbfs:/databricks-datasets/iot/iot_devices.json"
#dbutils.fs.ls(iotDeviceJSON)
df=spark.read.json(iotDeviceJSON)

# COMMAND ----------

df2=df.head(5)
display(df2)

# COMMAND ----------

df.write.json("/mnt/iotdata/fivedevice.json")

# COMMAND ----------

df2=df.select(df.battery_level,df.device_name,df.ip)
df2.coalesce(1).write.format("json").save("/mnt/iotdata/allDevWithSelCols.json")
#display(df2)

# COMMAND ----------

t=3
df=spark.range(t,(t*10)+1,t)
df.show()

# COMMAND ----------

data=[(1,1), (2,2), (None,3), (4,None)]
columns=["id", "number"]
testDF = sqlContext.createDataFrame(data, columns)
testDF.show()

# COMMAND ----------

from pyspark.sql.functions import *
tmp=testDF.withColumn('newColumn',coalesce(testDF['id'],testDF['number']))
tmp.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE iotdb
# MAGIC LOCATION "/mnt/iotdata"

# COMMAND ----------

tmp.write.saveAsTable("iotdb.devices")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iotdb.devices LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE iotdb.devices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE iotdb

# COMMAND ----------

dbutils.fs.unmount("/mnt/iotdata")

# COMMAND ----------


