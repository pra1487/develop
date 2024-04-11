import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("int_quest_1").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

df = spark.read.format('csv').option('header',True).load('file:///D://data/book1.csv')

# Renaming column
df = df.withColumnRenamed("Salary", 'Salary_Amount')

# adding column with case when funcs
df = df.withColumn("Dept_desc",when(df.Depart=='HR', 'Human Resource').when(df.Depart=='Admin', 'Administration')
                   .when(df.Depart == 'Eng', 'Engineering').otherwise(df.Depart))

# Drop column
df = df.drop(col('Depart'))

# Adding column
df = df.withColumn("Depart", when(col('Dept_desc')=='Human Resource', 'HR').when(col('Dept_desc')=='Administration', 'Admin')
                   .when(col('Dept_desc')=='Engineering','Eng').otherwise(col('Dept_desc')))
df.show()