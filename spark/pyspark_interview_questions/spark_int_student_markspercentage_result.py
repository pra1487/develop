import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master('local[*]').appName('intq').getOrCreate()

st_data = [(1,"Steve"),(2,"David"),(3,"John"),(4,"Shree"),(5,"Helen")]
st_cols = ["Id","Name"]

st_df = spark.createDataFrame(st_data,st_cols)
st_df.show()

marks_data = [(1,"SQL",90),(1,"PySpark",100),(2,"SQL",70),(2,"PySpark",60),
              (3,"SQL",30),(3,"PySpark",20),(4,"SQL",50),(4,"PySpark",50),(5,"SQL",45),(5,"PySpark",45)]
marks_cols = ["Id","Subject","Mark"]
marks_df = spark.createDataFrame(marks_data, marks_cols)
marks_df.show()

total_marks_df = marks_df.groupBy('ID').agg((sum('Mark')/count('*')).cast(IntegerType()).alias('marks_percentage'))
total_marks_df.show()

join_df = total_marks_df.join(st_df, total_marks_df.ID == st_df.Id).drop(st_df.Id).\
    select(total_marks_df.ID, st_df.Name, total_marks_df.marks_percentage)

join_df.show()
join_df.printSchema()

result_df = join_df.withColumn('Result', when(join_df.marks_percentage >= 70, 'Distinction').\
                               when((join_df.marks_percentage <= 69) & (join_df.marks_percentage >= 60),'First_Class').\
    when((join_df.marks_percentage<=59) & (join_df.marks_percentage>=50), 'Second_Class').\
    when((join_df.marks_percentage<=49) & (join_df.marks_percentage>=40), 'Third_Class').\
    when(join_df.marks_percentage<=39, 'Fail').otherwise(lit(None)))
result_df.show()
