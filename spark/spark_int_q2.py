import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("int_quest_1").master("local[*]").getOrCreate()
sc = spark.sparkContext()
sc.setLogLevel('Error')
