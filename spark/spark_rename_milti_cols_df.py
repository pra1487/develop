import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('renaming_mult_cols').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

df = spark.read.csv('file:///D://data/txns.txt', header=True)
df.show(2)

cols_to_be_rename = [('txnno', 'txnno_1'),
                     ('txndate', 'txndate_1'),
                     ('custno', 'custno_1'),
                     ('amount', 'amount_1'),
                     ('category', 'category_1'),
                     ('product', 'product_1'),
                     ('city', 'city_1'),
                     ('state', 'state_1'),
                     ('spendby', 'spendby_1')]


for x in cols_to_be_rename:
    df = df.withColumnRenamed(x[0], x[1])

df.show(3)

df1 = df.withColumnRenamed('txnno_1', 'txnno_2').withColumnRenamed('txndate_1','txndate_2').withColumnRenamed('city_1','city_2')
df1.show(3)