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
import pandas as pd
from urllib.request import urlopen
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data1 = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

data2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]

cols = 'emp_name string, depart string, state string, Salary long, age int, bonus int'


df1 = spark.createDataFrame(data1, cols)
df2 = spark.createDataFrame(data2, cols)

union_df = df1.unionAll(df2)
print(union_df.count())

union_df1 = df1.union(df2)
print(union_df1.count())