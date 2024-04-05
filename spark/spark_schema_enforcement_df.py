import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('schema_enforcement').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

# inferring schema from data
df = spark.read.csv('file:///D://data/orders.csv', header=True, inferSchema=True, samplingRatio=.1)
df.printSchema()

# Enforcing defined schema
# if the data type missmatch that column should have nulls

cols = 'order_id int, order_date date, order_customer_id int, order_status string'
df2 = df = spark.read.csv('file:///D://data/orders.csv', header=True, schema=cols)
df2.printSchema()

''' 
samplingRatio:
 - .1 means 10$ of random data should be scan for define schema
 - .001 means 0.1% of the random data should be scan
'''
