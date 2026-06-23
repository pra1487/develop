from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()
print(spark)

from urllib.request import urlopen

url = 'https://randomuser.me/api/0.8/?results=200'

data = [
    ("Banana", "USA", 100),
    ("Banana", "China", 400),
    ("Orange", "USA", 200),
    ("Orange", "China", 300),
    ("Banana", "USA", 150)
]

columns = ["Product", "Country", "Sales"]

df = spark.createDataFrame(data, columns)
df.show()

pivot_df = df.groupBy('Product').pivot('Country').sum('Sales')
pivot_df.show()

p2 = df.groupBy('Country').pivot('Product').sum('Sales')
p2.show()
