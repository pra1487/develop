import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [(1, 'A', 20, '31|32|34'),(2, 'B', 21, '21|32|43'),(3, 'C', 22, '21|32|11'),(4, 'D', 23, '10|12|12')]
cols = 'id int, name string, age int, marks string'

df = spark.createDataFrame(data, cols)
df.show()

df1 = df.withColumn('physics', split(col('marks'), '\\|').getItem(0))\
        .withColumn('chemistry', split(col('marks'), '\\|').getItem(1))\
        .withColumn('Maths', split(col('marks'), '\\|').getItem(2)).drop('marks')
df1.show()

df3 = df.withColumn('marks', explode(split(col('marks'),'\\|')))
df3.show()