import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('intq').master('local[*]').getOrCreate()

data=[(1,'2024-01-01',"I1",10,1000),(2,"2024-01-15","I2",20,2000),(3,"2024-02-01","I3",10,1500),
      (4,"2024-02-15","I4",20,2500),(5,"2024-03-01","I5",30,3000),(6,"2024-03-10","I6",40,3500),
      (7,"2024-03-20","I7",20,2500),(8,"2024-03-30","I8",10,1000)]
schema=["SOId","SODate","ItemId","ItemQty","ItemValue"]

df = spark.createDataFrame(data, schema)
df.show()

df = df.withColumn('SODate', col('SODate').cast(DateType()))
df.printSchema()

df1 = df.select(month(col('SODate')).alias('Month'), year(col('SODate')).alias('Year'), 'ItemValue')
df1.show()

from pyspark.sql.window import Window

df2 = df1.groupBy('Month','Year').agg(sum('ItemValue').alias('Total_Sale'))
df2.show()

df3 = df2.withColumn('Prev_month_sale', lag(col('Total_Sale'),1).over(Window.orderBy(col('Month'),col('Year'))))
df3.show()

df4 = df3.select('*', ((col('Total_Sale')-col('Prev_month_sale'))*100/col('Total_Sale')).alias('Sales_Percentage'))
df4.show()