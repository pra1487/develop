"""
1. spark session creation
2. df creation
3. groupby, agg, collect_list, count, sum, alias
4. withColumn, withColumnRenamed, when-otherwise
5. null drop (how = any) (how = 'any', thresh=2) (how='any', subset=[])
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
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Practice').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

empty_rdd = sc.emptyRDD()

cols = 'name string, is int, age int, dept string'

df = spark.createDataFrame(empty_rdd, cols)
df.show()
