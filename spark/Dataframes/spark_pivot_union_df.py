import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('pivot_union').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

schema1 = 'emp_name string, depart string, state string, Salary long, age int, bonus int'

df1 = spark.createDataFrame(simpleData, schema=schema1)
#df1.show()

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]

schema2 = 'emp_name string, depart string, state string, Salary long, age int, bonus int'

df2 = spark.createDataFrame(simpleData2, schema=schema2)
#df2.show()

union_df = df1.unionAll(df2)
union_df.show()

pivot_df = union_df.groupby('depart').pivot('state').sum('Salary')
pivot_df.show()