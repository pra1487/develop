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

spark = SparkSession.builder.appName('practice_udf').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]

cols = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

emp_df = spark.createDataFrame(emp, cols)
emp_df.show(2, False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptCols = ["dept_name","dept_id"]

dep_df = spark.createDataFrame(dept, deptCols)
dep_df.show()
