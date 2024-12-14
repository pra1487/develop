import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from urllib.request import urlopen

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()

prod_data = [(5,), (6,)]
prod_cols = "product_key int"
prod_df = spark.createDataFrame(prod_data, prod_cols)










