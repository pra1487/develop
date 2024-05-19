import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

schema = StructType([
    StructField('txnno',LongType(),True),
    StructField('txndate',StringType(),True),
    StructField('custno', LongType(), True),
    StructField('amount',FloatType(), True),
    StructField('category', StringType(),True),
    StructField('product', StringType(),True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('spendBy', StringType(), True)
])

path = 'file:///D://data/txns1'

df = spark.read.option('header', True).option('inferschema', True).csv(f"{path}")

if (schema == df.schema):
    df.write.mode('overwrite').save('D://data/Writedata/19May2024/txns_data/')
else:
    df = spark.read.option('header', False).schema(schema).csv(f'{path}')
    df.write.mode('overwrite').save('D://data/Writedata/19May2024/txns_data/')
