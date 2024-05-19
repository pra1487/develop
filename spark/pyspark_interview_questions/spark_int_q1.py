import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("interview Q1").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("Error")

data = [('a','aa',1),
('a','aa',2),
('b','bb',3),
('b','bb',4),
('b','bb',5)]

schema1 = ['col1', 'col2', 'col3']

df = spark.createDataFrame(data, schema=schema1)
df.show(truncate=False)

print('dataFrame')
final_df = df.groupby('col1', 'col2').agg(collect_list('col3').alias('result_col'))
final_df = final_df.withColumn('count', size(col('result_col')))
final_df.show()

# in spark.sql

df.createOrReplaceTempView("df")
print("spark.sql")
final_df_sql = spark.sql("select col1, col2, collect_list(col3) as result_col from df group by col1, col2")
final_df_sql.show()
