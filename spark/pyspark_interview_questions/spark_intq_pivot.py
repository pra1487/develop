import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('pivot').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [('Product1', 100, 150, 200),
        ('Product2', 200, 250, 300),
        ('Product1', 300, 350, 400)]

cols = ['Product', 'Sales_jan','Sales_feb','Sales_mar']

df = spark.createDataFrame(data, cols)

pivot_df = df.selectExpr('Product',
                         "stack(3, 'Jan', Sales_jan, 'Feb', 'Sales_feb', 'Mar', Sales_mar) as (Month, Sales)")
pivot_df.show()