import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [
    (1, "Sagar", 23, "Male", 68.0),
    (2, "Kim", 35, "Female", 90.2),
    (3, "Alex", 40, "Male", 79.1),
]

cols = "Id int,Name string,Age int,Gender string,Marks float"

df = spark.createDataFrame(data, cols)
df.show()
data_types = df.dtypes
data_types_set = set([i[1] for i in data_types])
print(data_types_set)

for i in data_types_set:
    cols = []
    for j in data_types:
        if (i==j[1]):
            cols.append(j[0])
    df.select(cols).write.mode('overwrite').save(f'D://data/Writedata/19May2024/{i}')
