import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('window_funcs').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

window_spc = Window.partitionBy('department').orderBy('salary')

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]

df = spark.createDataFrame(simpleData, columns)
df.show()

df1 = df.withColumn('dense_rank', dense_rank().over(window_spc))\
    .withColumn('rank', rank().over(window_spc))\
    .withColumn("row_number", row_number().over(window_spc))
df1.show()


