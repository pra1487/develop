import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [Row(UserID=1, PurchaseDate='2023-01-05'),
        Row(UserID=1, PurchaseDate='2023-01-10'),
        Row(UserID=2, PurchaseDate='2023-01-03'),
        Row(UserID=3, PurchaseDate='2023-01-12')]

sales_df = spark.createDataFrame(data)
sales_df = sales_df.withColumn('PurchaseDate', to_date('PurchaseDate'))
sales_df.show()

my_window = Window.partitionBy('UserID').orderBy('PurchaseDate')

result_df = sales_df.withColumn('dnsrank', dense_rank().over(my_window))
result_df.show()

final_df = result_df.filter(col('dnsrank')==1).drop('dnsrank')
final_df.show()

# Or

result_df_1 = sales_df.groupBy('UserID').agg(min('PurchaseDate').alias('FirstPurchaseDate'))
result_df_1.show()