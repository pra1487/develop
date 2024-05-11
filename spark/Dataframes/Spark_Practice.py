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
\+
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
import pandas as pd
from urllib.request import urlopen


spark = SparkSession.builder.appName('Practice').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]

cols = 'Name string, Dept string, Bonus int'

df = spark.createDataFrame(data, cols)

from pyspark.sql.window import Window
my_wind = Window.partitionBy('Dept').orderBy(col('Bonus').desc())
my_wind2 = Window.orderBy(col('Bonus'))

df2 = df.withColumn('next_sal', lag('Bonus',1).over(my_wind2)).withColumn('Bonus_diff', (col('Bonus')-col('next_sal')))\
    .withColumn('new_col', lit('new_val').cast(StringType()))
df2.show()

