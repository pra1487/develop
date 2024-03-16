import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType

spark = SparkSession.builder.appName('Coalesce').master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("Error")

data = [("3.07","3.05"),
        ("3.06","3.06"),
        ("3.09",None),
        (None,None),
        (None,"3.06"),
        (None,None)
       ]
schema = ['a','b']
df = spark.createDataFrame(data, schema=schema)
df.show()

df1 = df.withColumn("c", col('a'))
df1.show()

df2 = df1.withColumn("d", coalesce(col('a'),col('b'), col('c')).cast(FloatType()))
df2.show()