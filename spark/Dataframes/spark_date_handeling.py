import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from urllib.request import urlopen

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

cols = 'order_id long, order_date date, cust_id long, order_Status string'

df = spark.read.format('csv')\
    .schema(cols)\
    .load('file:///D:/data/Orders_sample1.csv')
# df.show()

# Spark will recognize the yyyy-mm-dd date format only.

df1 = spark.read.format('csv')\
    .schema(cols)\
    .option('dateFormat', 'MM-dd-yyyy')\
    .load('file:///D:/data/Orders_sample2.csv')
#df1.show()

# deals with withColumn

cols1 = 'order_id long, order_date string, cust_id long, order_Status string'

df3 = spark.read.format('csv').schema(cols1).load('file:///D:/data/Orders_sample1.csv')
#df3.show()
df3.printSchema()

df4 = df3.withColumn('order_date', to_date('order_date', 'yyyy-MM-dd'))
df4.show()
df4.printSchema()

