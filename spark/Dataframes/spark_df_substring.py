import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('spark df substring').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data = [(1,"20200828"),(2,"20180525")]

columns=["id","date"]

df = spark.createDataFrame(data, columns)
df.show()

df1 = df.withColumn('year', substring(col('date'), 1,4))\
    .withColumn('Month', substring(col('date'), 5,2))\
    .withColumn('day', substring(col('date'),7,2))
df1.show()

df2 = df.select('date', substring(col('date'), 1,4).alias('Year'),
                substring(col('date'),5,2).alias('Month'),
                substring(col('date'),7,2).alias('Day'))
df2.show()
df2.printSchema()