import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

""" Generate seq numbers for each Group """

spark = SparkSession.builder.appName("gen_seq_num").master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data = [Row(GroupID='A', Date='2023-01-01'),
        Row(GroupID='A', Date='2023-01-02'),
        Row(GroupID='B', Date='2023-01-01'),
        Row(GroupID='B', Date='2023-01-03')]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

df = df.withColumn('Date', col('Date').cast('date'))
df.printSchema()

my_window = Window.partitionBy('GroupID').orderBy('Date')

result_df = df.withColumn("SeqNumber", row_number().over(my_window))
result_df.show()