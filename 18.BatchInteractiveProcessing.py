# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/structured-streaming/events/file-0.json

# COMMAND ----------

# MAGIC %md
# MAGIC # Batch/Interactive Processing

# COMMAND ----------

from pyspark.sql.types import *

jsonSchema=StructType([
    StructField("time",TimestampType(),True),
    StructField("action",StringType(),True)
])

dataPath="/databricks-datasets/structured-streaming/events/"

staticInputDF=(spark.read.schema(jsonSchema).json(dataPath))

display(staticInputDF)

# COMMAND ----------

from pyspark.sql.functions import *
staticCountsDF=(
    staticInputDF.groupBy(
        staticInputDF.action,window(staticInputDF.time,"1 hour")
    ).count()
)
staticCountsDF.cache()
staticCountsDF.createOrReplaceTempView("static_counts")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT action,sum(count) AS total_count FROM static_counts GROUP BY action

# COMMAND ----------

sampleData = (("Mujahed", "Sales", 3000), \
    ("Megha", "Sales", 4600),  \
    ("Lakshmi", "Sales", 4100),   \
    ("Harshit", "Finance", 3000),  \
    ("Mujahed", "Sales", 3000),    \
    ("Anjali", "Finance", 3300),  \
    ("Yash", "Finance", 3900),    \
    ("Bhol", "Marketing", 3000), \
    ("Kinshuk", "Marketing", 2000),\
    ("Santhosh", "Sales", 4100) \
  )
columns=["employee_name","department","salary"]
df=spark.createDataFrame(data=sampleData,schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec=Window.partitionBy("department").orderBy("salary")
df.withColumn("row_num",row_number().over(windowSpec)).show(truncate=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Stream Processing

# COMMAND ----------


