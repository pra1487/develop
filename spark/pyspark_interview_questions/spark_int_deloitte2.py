import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('intq').master('local[*]').getOrCreate()

data = [('IT', 'M'),('IT', 'F'),('IT', 'M'),('IT', 'M'),
        ('HR', 'F'),('HR', 'M'),('HR', 'F'),('HR', 'F'),('HR', 'M'),
        ('Sales', 'M'),('Sales', 'F'),('Sales', 'M'),('Sales', 'F'),('Sales', 'M'),('Sales', 'M')]
cols = ['DeptName', 'Gender']

df = spark.createDataFrame(data, cols)
df.show()

df1 = df.select('DeptName', when(col('Gender')=='M', 1).otherwise(lit(0)).alias('Male'),\
                when(col('Gender')=='F', 1).otherwise(lit(0)).alias('Female'))
df1.show()

final_df = df1.groupBy('DeptName').agg(count('DeptName').alias('Total_emp_count'),sum('Male').alias('male_count'),\
                                       sum('Female').alias('Female_count'))
final_df.show()

#-----------------------

df.createOrReplaceTempView('df')
spark.sql("""select DeptName, count(*) as total_emp_count, sum(female) as female_count, sum(male) as male_count
          from (select *, case when Gender = 'M' then 1 else 0 end as male, case when Gender = 'F' then 1 else 0 end as female
          from df) group by DeptName""").show()