import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder.appName('regexp_replace').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

address = [(1,"14851 Jeffrey Rd","Delaware"),
    (2,"43421 Margarita St","New York"),
    (3,"13111 Siemon Ave","California")]

columns = 'id int, address string, state string'

df = spark.createDataFrame(address, schema=columns)
df.show()

df1 = df.withColumn('address', when(col('address').endswith('Rd'), regexp_replace(col('address'), 'Rd', 'Road'))
                    .when(col('address').endswith('St'), regexp_replace(col('address'), 'St', 'Street'))
                    .when(col('address').endswith('Ave'), regexp_replace(col('address'), 'Ave', 'Avenue'))
                    .otherwise(col('address')))

df1.show(truncate=False)

df2 = df1.withColumn('address', translate('address', '123', 'ABC'))
df2.show()


