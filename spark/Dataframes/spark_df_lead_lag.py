import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('lead_lag').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )

cols = "name string, dept string, salary int"

df = spark.createDataFrame(data, cols)
df.show()

from pyspark.sql.window import Window

window1 = Window.partitionBy('dept').orderBy(col('salary').desc())

lag_df = df.withColumn('lag_sal', lag('salary', 1).over(window1).cast(IntegerType()))\
    .withColumn("sal_diff", (col('lag_sal')-col('salary').cast("integer")))\
    .withColumn("lead_sal", lead('salary',1).over(window1).cast(IntegerType()))
lag_df.show()

