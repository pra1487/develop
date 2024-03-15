import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("inter_ques_3").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("Error")

df = spark.read.csv('file:///D://data/book1.csv', header=True)
print(df.count())
df.show(truncate=False)

# Handeling Null values

df_all_null_rmv = df.na.drop() # dropping all null.
df_all_null_rmv.show()




