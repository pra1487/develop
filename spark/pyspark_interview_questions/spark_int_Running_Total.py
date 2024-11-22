import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()

simpleData = [(1,"James", "Sales", 3000),
    (2,"Michael", "Sales", 4600),
    (3,"Robert", "Sales", 4100),
    (4,"Maria", "Finance", 3000),
    (5,"James", "Sales", 3000),
    (6,"Scott", "Finance", 3300),
    (7,"Jen", "Finance", 3900),
    (8,"Jeff", "Marketing", 3000),
    (9,"Kumar", "Marketing", 2000),
    (10,"Saif", "Sales", 4100)
  ]
cols = 'Id int, name string, dept string, salary int'

df = spark.createDataFrame(simpleData, cols)
df.show()

df1 = df.withColumn('Running Total', sum('salary').over(Window.orderBy('Id')))
df1.show()