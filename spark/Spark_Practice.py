import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Localpractice').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

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
11. empty df, StructType
"""

df = spark.read.csv('file:///D://data/Book1.csv', header=True)
df.show()

columns = ["name","languagesAtSchool","currentState"]

data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")]

df1 = spark.createDataFrame(data, schema=columns)
df1.show()

df2 = df1.withColumn('first_name', when(split('name', ',')[0]=='', split('name',',')[1])
                     .when(split('name',',')[0]!='', split('name',',')[0])
                     .otherwise(split('name',',')[0]))
df2.show()

df3 = df1.withColumn('lang_count', size(split(concat_ws(',', 'languagesAtSchool'), ',')))
df3.show()