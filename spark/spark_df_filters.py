import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('df_filters').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]

columns = StructType([
    StructField('name', StructType([
        StructField('fname', StringType(),True),
        StructField('mname', StringType(), True),
        StructField('lname', StringType(), True)
    ])),
    StructField('lang', ArrayType(StringType(), True)),
    StructField('State', StringType(), True),
    StructField('Gend', StringType(), True)
])

df = spark.createDataFrame(data, schema=columns)
df.show()
'''
df1 = df.withColumn('lang_count', size(split(concat_ws(',', 'languanges'), ',')))
df1.show()

df1 = df.filter(col('Gender')!='M').filter(col('State')!='OH')
df1.show()

li = ['OH', 'NY']

filter_df = df.filter(df.State.isin(li)).sort(col('State'), asc=False)
filter_df.show()

filter_df1 = df.filter(df.State.startswith('O'))
filter_df1.show()

filter_df2 = df.filter(col('State').startswith('O'))
filter_df2.show()

df2 = df.withColumn("java_present", array_contains(col('lang'), 'Java'))
df2.show()

df3 = df.where(array_contains(col('lang'), 'Java'))
df3.show()

'''
