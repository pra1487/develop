import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('df_filters').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

""" To find the customer who are oder all the products"""

prod_data = [(5,), (6,)]
prod_cols = "product_key int"

order_data = [(1,5),(2,6),(3,5),(3,6),(1,6)]
order_cols = "customer_id int, product_key int"

product_df = spark.createDataFrame(prod_data, prod_cols)
product_df.show()

order_df = spark.createDataFrame(order_data, order_cols)
order_df.show()

dist_product = product_df.agg(countDistinct(col('product_key')).alias('cnt_products'))
dist_orders = order_df.groupby('customer_id').agg(countDistinct(col('product_key')).alias('cnt_products'))

df = dist_product.join(dist_orders, dist_product.cnt_products==dist_orders.cnt_products, 'inner').select(dist_orders.customer_id)
df.show()



