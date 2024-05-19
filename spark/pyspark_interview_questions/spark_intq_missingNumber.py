import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Missing_Number').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [(1,),(2,),(4,),(6,),(7,),(8,),(10,)]
cols = ["numbers"]

num_df = spark.createDataFrame(data, cols)
#num_df.show()

all_num_df = spark.range(1,11).toDF('numbers')
#all_num_df.show()

join_df = all_num_df.join(num_df, 'numbers', 'left_anti')\
    .withColumn('missing_numbers', col('numbers').cast(IntegerType()))\
    .drop('numbers')
join_df.show()
join_df.printSchema()