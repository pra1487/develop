"""
1. spark session creation
2. df creation
3. groupby, agg, collect_list, count, sum, alias
4. withColumn, withColumnRenamed, when-otherwise
5. null drop (how = 'any') (how = 'any', thresh=2) (how='any', subset=[])
6. null fill (value=0)(value='')
7. filters on df, select columns
8. withColumn coalesce cast
9. split, size, concat_ws
10. distinct, dropDups, sort
11. empty df, StructType, lit, explode
12. pivot, union, regexp_replace, translate
13. avg, max, min, first, last, avg, count, sum, sum_distinct, count_distinct, approx_count_distinct, collect_list, collect_set
14. substring
15. to_timestamp, to_date, year, month, dayofmonth
16. translate, split
17. rename multiple columns
18. jdbc read from spark
19. window func, web api data process
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from urllib.request import urlopen

spark = SparkSession.builder.master('local[*]').appName('Interview').getOrCreate()

data = [Row(UserID=1, PurchaseDate='2023-01-05'),
        Row(UserID=1, PurchaseDate='2023-01-10'),
        Row(UserID=2, PurchaseDate='2023-01-03'),
        Row(UserID=3, PurchaseDate='2023-01-12')]

sales_df = spark.createDataFrame(data)
sales_df.show()

from pyspark.sql.window import Window

my_window = Window.partitionBy('UserID').orderBy('PurchaseDate')

df = sales_df.withColumn('dense_rank', dense_rank().over(my_window))

result_df = df.filter(col('dense_rank')==1).drop('dense_rank')
result_df.show()