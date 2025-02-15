from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Json_process').master('local[*]').getOrCreate()

# Multi Array Json file read
df = spark.read.json('file:///D://data/ComplexData/MultiArrays1.json', multiLine=True)
df.show()
df.printSchema()

# Flattern Dataframe
df = df.withColumn('Students', explode(col('Students')))
df.printSchema()

df = df.withColumn('components', explode(col('Students.user.components')))
df.printSchema()