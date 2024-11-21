import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('intQ').master('local[*]').getOrCreate()

data1=[(1,"Ram","Male",100),(2,"Radhe","Female",200),(3,"John","Male",250)]
data2=[(101,"John","Male",100),(102,"Joanne","Female",250),(103,"Smith","Male",250)]
data3=[(1001,"Maxwell","IT",200),(2,"MSD","HR",350),(3,"Virat","IT",300)]
schema1=["Id","Name","Gender","Salary"]
schema2=["Id","Name","Gender","Salary"]
schema3=["Id","Name","DeptName","Salary"]
df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)
df3=spark.createDataFrame(data3,schema3)

if df1.schema == df3.schema:
    print("Yes schema matched")
else:
    all_cols = df1.columns+df3.columns
    uniq_cols = set(all_cols)
    print(uniq_cols)
    print(f"additional unmatched cols from both are: {[set(df1.schema)-set(df3.schema), set(df3.schema)-set(df1.schema)]}")
    for i in uniq_cols:
        if (i not in df1.columns):
            df1 = df1.withColumn(i, lit(None))
        if (i not in df3.columns):
            df3 = df3.withColumn(i,lit(None))
df1.show()
df3.show()



