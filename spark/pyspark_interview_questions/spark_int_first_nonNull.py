import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('intq').getOrCreate()

data = [('Goa','','AP'),('','AP',None),(None,'','Bglr')]
cols = ['city1','city2','city3']

df = spark.createDataFrame(data, cols)
df.show()

result_df = df.withColumn('City', coalesce(when(col('city1')=='',None).otherwise(col('city1')),\
                                           when(col('city2')=='',None).otherwise(col('city2')),\
                                           when(col('city3')=='',None).otherwise(col('city3'))))\
    .select('City')

result_df.show()