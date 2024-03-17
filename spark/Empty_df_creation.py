import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('EmptyDF').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

empty_rdd = sc.emptyRDD()

columns = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('Depart', StringType(), True)
])

df = spark.createDataFrame(empty_rdd, schema=columns)
df.show()