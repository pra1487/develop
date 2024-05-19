import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import Row

""" Finding rolling average """

spark = SparkSession.builder.appName('interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [Row(Date='2023-01-01', ProductID=100, QuantitySold=10),
        Row(Date='2023-01-02', ProductID=100, QuantitySold=15),
        Row(Date='2023-01-03', ProductID=100, QuantitySold=20),
        Row(Date='2023-01-04', ProductID=100, QuantitySold=25),
        Row(Date='2023-01-05', ProductID=100, QuantitySold=30),
        Row(Date='2023-01-07', ProductID=100, QuantitySold=35),
        Row(Date='2023-01-08', ProductID=100, QuantitySold=40),
        Row(Date='2023-01-09', ProductID=100, QuantitySold=45)]

sales_df = spark.createDataFrame(data)

sales_df = sales_df.withColumn('Date', to_date(col('Date')))
sales_df.printSchema()

my_window = Window.partitionBy('ProductID').orderBy('Date').rowsBetween(-6,0)

rollingAvg = sales_df.withColumn('7DayAvg', avg('QuantitySold').over(my_window))
rollingAvg.show()