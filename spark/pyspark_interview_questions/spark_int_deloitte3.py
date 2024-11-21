import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('intq').master('local[*]').getOrCreate()

data = [('Joanne',"040-20215632"),('Tom',"044-23651023"),('John',"086-12456782")]
schema = ["name","phone"]

df = spark.createDataFrame(data,schema)
df.show()

split_col = split(col('phone'),'-')

df1 = df.select('name', split_col[0].alias('stdcode'), split_col[1].alias('phone_number'))
df1.show()