import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Intq').master('local[*]').getOrCreate()

data = [('Alice', 'Badminton,Tennis'),('Bob', 'Tennis,Cricket'),('Julie', 'Cricket, Carroms')]
cols = 'name string, game string'

df = spark.createDataFrame(data, cols)
df.show()

result_df = df.select(col('name'), explode(split(col('game'), ',')))
result_df.show()
