import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Jdbc read').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')
