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

filter_df1 = df.filter(col('Depart')=='HR')
filter_df1.show()

filter_df2 = df.filter(col('Depart')=='HR').filter(col('Salary')>10000).filter(col('Experience').isNull())
filter_df2.show()

filter_select_df = df.filter(col('Depart')=='HR').select(['Name','Age','Salary']).filter(col('Salary').isNotNull())
filter_select_df.show()

group_agg_df = df.groupby(col('Depart')).agg(count('ID').alias('No_Of_Emps'))
group_agg_df.show()

group_df_df1 = df.groupby(col('Depart')).agg(sum(col('Salary')).alias('Total_Salary'))
group_df_df1.show()

group_df_df2 = df.agg(sum(col('Salary')).alias('Total_Salary'))
group_df_df2.show()

