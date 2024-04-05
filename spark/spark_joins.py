import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Spark_Joins').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

emp_df = spark.createDataFrame(emp, empColumns)
emp_df.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]

dep_df = spark.createDataFrame(dept, deptColumns)
dep_df.show(truncate=False)

inner_df = emp_df.join(dep_df, col('emp_dept_id')==col('dept_id'), 'inner').select("emp_id","name",'dept_name')
inner_df.show()

left_df = emp_df.join(dep_df, emp_df.emp_dept_id==dep_df.dept_id, 'left').drop('dept_id').na.fill(value='No dep in dep_table')
left_df.show()

self_join_df =  emp_df.alias('emp1').join(emp_df.alias('emp2'),\
                                          col('emp1.superior_emp_id')==col('emp2.emp_id'), 'inner')\
    .select(col('emp1.emp_id'), col('emp1.name'), col('emp1.superior_emp_id'), col('emp2.name').alias('Superior_name'))
self_join_df.show()