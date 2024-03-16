import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("interview_questions").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("error")

df = spark.read.csv("file:///D:/data/book1.csv", header=True)
df.show()

# Filling Nulls

df_null_fill = df.na.fill(value=0).na.fill(value='Missing')
df_null_fill.show()

# Filtering df
salary_filter_df = df.filter(col('Salary')>=60000).na.fill(value='Missing')
salary_filter_df.show()

