import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('intq').master('local[*]').getOrCreate()

data=[(0,0,'start',0.712),(0,0,'end',1.520),(0,1,'start',3.140),(0,1,'end',4.120),
      (1,0,'start',0.550),(1,0,'end',1.550),(1,1,'start',0.430),(1,1,'end',1.420),
      (2,0,'start',4.100),(2,0,'end',4.512),(2,1,'start',2.500),(2,1,'end',5.000)]

schema=["Machine_id","processid","activityid","timestamp"]

df = spark.createDataFrame(data,schema)
df.show()

df1 = df.withColumn('prev_timestamp', lag(col('timestamp'),1).over(Window.partitionBy('Machine_id','processid').orderBy('activityid')))\
    .filter(col('prev_timestamp').isNotNull())
df1.show()

df2 = df1.withColumn('time_diff', (col('prev_timestamp')-col('timestamp')))
df2.show()

df3 = df2.groupBy('Machine_id').agg(round(avg('time_diff'),3).alias('avrg_process_time'))
df3.show()