import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Interview').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

data = [('1',100),('2',150),('3',None),('4',200),('5',None)]

df = spark.createDataFrame(data, ['sale_id', 'amount'])
df.show()

avrg_sal_amt = df.na.drop().agg(mean(col('amount'))).first()[0]
print(avrg_sal_amt)

result_df = df.na.fill(value=avrg_sal_amt)
result_df.show()
