import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.udf import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [Row(Date='2023-01-01', VisitorID=101),
        Row(Date='2023-01-01', VisitorID=102),
        Row(Date='2023-01-01', VisitorID=101),
        Row(Date='2023-01-02', VisitorID=103),
        Row(Date='2023-01-02', VisitorID=101)]

visitor_df = spark.createDataFrame(data)
visitor_df = visitor_df.withColumn('Date', to_date('Date'))
visitor_df.show()

result_df = visitor_df.groupBy('Date').agg(count_distinct('VisitorID').alias('Unique_VisitorID'))
result_df.show()
