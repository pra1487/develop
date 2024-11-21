import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('intq').master('local[*]').getOrCreate()

data1=[(1,"A",1000,"IT"),(2,"B",1500,"IT"),(3,"C",2500,"IT"),(4,"D",3000,"HR"),(5,"E",2000,"HR"),(6,"F",1000,"HR")
       ,(7,"G",4000,"Sales"),(8,"H",4000,"Sales"),(9,"I",1000,"Sales"),(10,"J",2000,"Sales")]
schema1=["EmpId","EmpName","Salary","DeptName"]

df = spark.createDataFrame(data1,schema1)
df.show()

res_df = df.withColumn('dense_rank', dense_rank().over(Window.partitionBy('DeptName').orderBy(col('Salary').desc())))\
       .filter(col('dense_rank')==1).drop('dense_rank')
res_df.show()
