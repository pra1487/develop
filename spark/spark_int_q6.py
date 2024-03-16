import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

columns = ["name","languagesAtSchool","currentState"]

data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")]

df = spark.createDataFrame(data, schema=columns)
df.show()

df1 = df.withColumn("first_name", split(col('name'), ',')[0]) \
        .withColumn("lastname", when(split(col('name'), ',')[1] == '', split(col('name'), ',')[2])
                                .when(split(col('name'), ',')[1] != '', split(col('name'), ',')[1])
                                .otherwise(col('name')))
df1.show()

df2 = df.withColumn('lang', concat_ws(',', col('languagesAtSchool')))
df2.show()

df3 = df.withColumn('lang_count', size(split(concat_ws(',', col('languagesAtSchool')), ',')))
df3.show()


