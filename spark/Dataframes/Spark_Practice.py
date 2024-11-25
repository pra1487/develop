import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()

path = 'file:///D://data/txns.txt'

data=[(1,'2024-01-01',"I1",10,1000),(2,"2024-01-15","I2",20,2000),(3,"2024-02-01","I3",10,1500),
      (4,"2024-02-15","I4",20,2500),(5,"2024-03-01","I5",30,3000),(6,"2024-03-10","I6",40,3500),
      (7,"2024-03-20","I7",20,2500),(8,"2024-03-30","I8",10,1000)]
schema=["SOId","SODate","ItemId","ItemQty","ItemValue"]

df = spark.createDataFrame(data, schema)
df.show()

df1 = df.withColumn('SODate', to_date(col('SODate'), 'yyyy-MM-dd'))\
       .select('SOId', month(col('SODate')).alias('Month'), year(col('SODate')).alias('Year'), 'ItemValue')
df1.show()

df2 = df1.groupBy('Month', 'Year').agg(sum('ItemValue').alias('Total_sales'))\
    .withColumn('Prev_sal', lag(col('Total_sales')).over(Window.orderBy('Month')))\
    .withColumn('sales_percentage', (col('Total_sales')-col('Prev_sal'))/col('Total_sales')*100)
df2.show()