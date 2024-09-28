import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from urllib.request import urlopen

spark = SparkSession.builder.master('local[*]').appName('Practice').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')



