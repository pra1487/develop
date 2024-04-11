import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('to_timestamp').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [('James','sales',"2019-06-24 12:01:19.000"),
        ('david','Finance',"2024-05-22 12:01:19.000"),
        ('James','Marketing',"2023-03-23 12:01:19.000"),
        ('James','sales',"2022-02-12 12:01:19.000")]

cols = ['name', 'dept', 'time_stamp']

df = spark.createDataFrame(data, cols)
df.show(truncate=False)
df.printSchema()

df1 = df.withColumn('time_stamp', to_timestamp(col('time_stamp')))
df1.show()
df1.printSchema()

df2 = df1.withColumn("date", to_date(col('time_stamp')))\
    .withColumn('year', year(col('date'))).withColumn('month', month(col('date')))\
    .withColumn('day', dayofmonth(col('date')))
df2.show()
df2.printSchema()
