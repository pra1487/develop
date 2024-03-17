import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('df_lit').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]
columns = 'name string, gender string, salary long'

df = spark.createDataFrame(data, schema=columns)
df.show()

df2 = df.select(col('*'), lit(1).alias('new_col1'))
df2.show()

df3 = df.withColumn('new_col2', when((col('salary')>=40000) & (col('salary')<=70000), lit(100)).otherwise(lit(200)))
df3.show()