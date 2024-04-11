import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('translate').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

address = [(1,"14851 Jeffrey Rd","Delaware"),
    (2,"43421 Margarita St","New York"),
    (3,"13111 Siemon Ave","California")]

cols = ['id', 'address', 'state']

df = spark.createDataFrame(address, cols)
df.show()

df1 = df.withColumn('address', translate(col('address'), 'JMS', '123'))
df1.show()

df2 = df.withColumn('pin', split(col('address'), ' ')[0])
df2.show()