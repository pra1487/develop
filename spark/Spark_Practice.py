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
19. window func,
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

address = [(1,"14851 Jeffrey Rd","Delaware"),
    (2,"43421 Margarita St","New York"),
    (3,"13111 Siemon Ave","California")]

cols = 'id int, address string, state string'

df = spark.createDataFrame(address, cols)
df.show()

df1 = df.withColumn('address', when(col('address').endswith('Rd'), regexp_replace(col('address'), 'Rd', 'Road'))\
                    .when(col('address').endswith('St'), regexp_replace(col('address'), 'St', 'Street'))\
                    .when(col('address').endswith('Ave'), regexp_replace(col('address'), 'Ave', 'Avenue'))\
                    .otherwise(col('address')))
df1.show()

df2 = df1.withColumn('address', translate(col('address'), 'JMS', '123'))
df2.show()