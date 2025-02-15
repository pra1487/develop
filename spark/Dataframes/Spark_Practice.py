from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date
from urllib.request import urlopen

spark = SparkSession.builder.master('local[*]').appName('practice').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

path = 'file:///D://data/book1.csv'
path1 = 'file:///D://data/txns.txt'

