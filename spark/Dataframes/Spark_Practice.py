import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from urllib.request import urlopen

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()

df = spark.read.json('file:///D://data/ComplexData/MultiArrays1.json', multiLine=True)
df = df.withColumn('Students', explode(col('Students')))
df = df.withColumn('')