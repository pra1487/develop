import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from urllib.request import urlopen

spark = SparkSession.builder.master('local[*]').appName('practice').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]

df = spark.createDataFrame(data, columns)
df.show()

final_df = df.select('id', substring(col('date'), 1,4).alias('Year'),
                     substring(col('date'),5,2).alias('Month'),\
                     substring())