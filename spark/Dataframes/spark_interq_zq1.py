import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Practice').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

df1 = spark.createDataFrame(['1','2','3'], StringType()).toDF('col1')
df1.show()

df2 = spark.createDataFrame(['1','2','3','4','5'], StringType()).toDF('col1')
df2.show()

max_df1 = df1.select(max(col('col1')).alias('col2'))
max_df1.show()

result_df = df2.join(max_df1, df2.col1==max_df1.col2, 'leftanti')
result_df.show()
