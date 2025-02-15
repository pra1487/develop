import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('word_count').master('local[*]').getOrCreate()

rdd1 = spark.sparkContext.textFile('file:///D://data/hello.txt')
rdd2 = rdd1.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y: x+y)
rdd5 = rdd4.repartition(4)
print(rdd5.getNumPartitions())
df = spark.createDataFrame(rdd5, schema='name string, count int')
df.show()

