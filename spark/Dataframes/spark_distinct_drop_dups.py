import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('inter Quest').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]

'''
columns = StructType([
    StructField('name', StringType(), True),
    StructField('dept', StringType(), True),
    StructField('salary', IntegerType(), True)
])
'''
schema = 'name string, dept string, salary int'

df = spark.createDataFrame(data, schema=schema)
df.show()
# df.printSchema()

df1 = df.distinct()
df1.show()

df2 = df.dropDuplicates(['salary']).sort('salary', asc=True)
df2.show()

print("Distinct count: {}".format(df.distinct().count()))

