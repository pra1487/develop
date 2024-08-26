import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data=[('John Doe','{"street": "123 Main St", "city": "Anytown"}'),('Jane Smith','{"street": "456 Elm St", "city": "Othertown"}')]

df=spark.createDataFrame(data,schema="name string,address string")
df.show()

df1 = df.withColumn('json_col', from_json(col('address'), 'street string, city string')).drop('address')
df1.show()

df2 = df1.withColumn('street', col('json_col').street.alias('Street'))\
    .withColumn('city', col('json_col').city.alias('City'))
df2.show()

