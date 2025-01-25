import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import date
from urllib.request import urlopen

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()

url = "https://randomuser.me/api/0.8/?results=200"

url_open = urlopen(url).read().decode('utf-8')
url_rdd = spark.sparkContext.parallelize([url_open])
url_df = spark.read.json(url_rdd)
url_df.printSchema()

url_df = url_df.withColumn('results', explode(col('results')))

url_df = url_df.select(col('results.user.name.first').alias('first_name'),
                       col('results.user.name.last').alias('last_name'),
                       col('results.user.phone').alias('phone'),
                       col('results.user.email').alias('mail_id'))
url_df.show(truncate=True)