import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row

""" UDF implementation """

spark = SparkSession.builder.appName('UDF').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [Row(UserID=4001, Age=17),
        Row(UserID=4002, Age=45),
        Row(UserID=4003, Age=65),
        Row(UserID=4004, Age=30),
        Row(UserID=4005, Age=80)]

df = spark.createDataFrame(data)
df.show()

def age_category(age):
        if age < 18:
                return 'Youth'
        elif age < 60:
                return 'Adult'
        else:
                return 'Senior'

age_udf = udf(age_category, StringType())

df = df.withColumn('AgeGroup', age_udf('Age'))
df.show()