import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('intq').master('local[*]').getOrCreate()

data = [(1,'John','ADF'),(1,'John','ADB'),(1,'John','PowerBI'),(2,'Joanne','ADF'),
        (2,'Joanne','SQL'),(2,'Joanne','Crystal Report'),(3,'Vikas','ADF'),(3,'Vikas','SQL'),
        (3,'Vikas','SSIS'),(4,'Monu','SQL'),(4,'Monu','SSIS'),(4,'Monu','SSAS'),(4,'Monu','ADF')]
schema = ["EmpId","EmpName","Skill"]

df = spark.createDataFrame(data,schema)
df.show()

df1 = df.groupBy('EmpName').agg(collect_list('Skill').alias('skills'))
df1.show()

final_df = df1.withColumn('skills', concat_ws(',',col('skills')))
final_df.show()