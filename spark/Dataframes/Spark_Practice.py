import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from urllib.request import urlopen
from datetime import date
from pyspark.sql.window import Window

spark = SparkSession.builder.master('local[*]').appName('practice').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

data = [('IT', 'M'),('IT', 'F'),('IT', 'M'),('IT', 'M'),
        ('HR', 'F'),('HR', 'M'),('HR', 'F'),('HR', 'F'),('HR', 'M'),
        ('Sales', 'M'),('Sales', 'F'),('Sales', 'M'),('Sales', 'F'),('Sales', 'M'),('Sales', 'M')]
cols = ['DeptName', 'Gender']

df = spark.createDataFrame(data, cols)
df.show()

df.createOrReplaceTempView('df')
spark.sql("""select DeptName, count(*) as total_emp_count, sum(female) as female_count, sum(male) as male_count
          from (select *, case when Gender = 'M' then 1 else 0 end as male, case when Gender = 'F' then 1 else 0 end as female
          from df) group by DeptName""").show()