import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Multi_delimiter_split').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = ["1,Virat\t35|Bangalore"]

df = spark.createDataFrame(data, 'string')
df.show()

split_col = split(df.value, ',|\t|\|')

result_df = df.withColumn('id', split_col.getItem(0).cast(IntegerType()))\
    .withColumn('name', split_col.getItem(1).cast(StringType()))\
    .withColumn('age', split_col.getItem(2).cast(IntegerType()))\
    .withColumn('city', split_col.getItem(3).cast(StringType()))\
    .drop('value')

result_df.show()
result_df.printSchema()