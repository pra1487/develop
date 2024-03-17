import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('explode_df').master('local[*]').enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

columns = StructType([
    StructField('name', StructType([
        StructField('fname', StringType(), True),
        StructField('mname', StringType(), True),
        StructField('lname', StringType(), True)
    ])),
    StructField('lang', ArrayType(StringType(), True)),
    StructField('State', StringType(), True),
    StructField('Gender', StringType(), True)
])

df = spark.createDataFrame(data, schema=columns)
df.show()
df.printSchema()

df1 = df.withColumn('lang', explode(col('lang')))
df1.show()
df1.printSchema()

df2 = df.select(col('name.fname').alias('fname'),
                col('name.mname').alias('mname'),
                col('name.lname').alias('lname'),
                col('lang'),
                col('state'), col('Gender'))
df2.show()
