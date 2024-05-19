import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('inter question').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

df = spark.read.csv('file:///D://data/book1.csv', header=True)
df.show()

df1 = df.select([count(i).alias(i+'(Total_Cnt)') for i in df.columns])
df1.show()

df2 = df.select([count(when(col(i).isNull(), i)).alias(i+"(Null_cnt)") for i in df.columns])
df2.show()