import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('spark_sql_agg').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

simpleData = [("James", "Sales", 3000),
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

cols = 'name string, dept string, salary int'

df = spark.createDataFrame(simpleData, cols)
df.show()

avg_sal = df.select(avg(col('salary')))
avg_sal = avg_sal.collect()[0][0]
print("Average Salary: {}".format(avg_sal))

max_sal = df.select(max(col('salary')))
max_sal = max_sal.collect()[0][0]
print('Max salary from table: {}'.format(max_sal))

min_sal = df.select(min(col('salary')))
min_sal = min_sal.collect()[0][0]
print('Min salary is: {}'.format(min_sal))

sum_distinct_sal = df.select(sum_distinct(col('salary')))
sum_distinct_sal = sum_distinct_sal.collect()[0][0]
print("Sum of distinct salary: {}".format(sum_distinct_sal))

apprx_cnt_distinct = df.select(approx_count_distinct(col('salary')))
apprx_cnt_distinct = apprx_cnt_distinct.collect()[0][0]
print('Approx number of distinct salary: {}'.format(apprx_cnt_distinct))

cnt_dist_salary = df.select(count_distinct(col('salary'))).collect()[0][0]
print("count of distinct salary: {}".format(cnt_dist_salary))

count_sal = df.select(count(col('salary')))
count_sal = count_sal.collect()[0][0]
print("Count of salary: {}".format(count_sal))

sum_sal = df.select(sum(col('salary')))
sum_sal = sum_sal.collect()[0][0]
print("Sum of salary: {}".format(sum_sal))

coll_list = df.select(collect_list(col('salary')))
coll_list.show(truncate=False)

coll_set = df.select(collect_set(col('salary')))
coll_set.show(truncate=False)

first_sal = df.select(first(col('salary')))
first_sal.show(truncate=False)

last_sal = df.select(last(col('salary')))
last_sal.show(truncate=False)



