import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('spark UDF').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

cols = 'id string, name string'

df = spark.createDataFrame(data, cols)
df.show()


# defining function
def conCase(x):
    resStr = ''
    strli = x.split()

    for i in strli:
        resStr = resStr+i[0].upper()+i[1:len(i)]+' '

    return resStr

# converting fun to UDF

myUDF = udf(lambda z: conCase(z), StringType())

df1 = df.withColumn('name', myUDF(col('name')))
df1.show()

def upstr(x):
    return x.upper()

# convert fun to udf

upperStr_udf = udf(lambda z: upstr(z), StringType())

df2 = df.withColumn('name', upperStr_udf(col('name')))
df2.show()