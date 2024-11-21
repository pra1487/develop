import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('intq').master('local[*]').getOrCreate()

data1=[(100,"Raj",None,1,"01-04-23",50000),
       (200,"Joanne",100,1,"01-04-23",4000),(200,"Joanne",100,1,"13-04-23",4500),(200,"Joanne",100,1,"14-04-23",4020)]
schema1=["EmpId","EmpName","Mgrid","deptid","salarydt","salary"]

salary_df = spark.createDataFrame(data1,schema1)
salary_df = salary_df.withColumn('salarydt', to_date(col('salarydt'),'dd-MM-yy'))\
    .withColumn('sal_year', year(col('salarydt')))\
    .withColumn('sal_month', date_format(col('salarydt'),'MMM'))

salary_df = salary_df.alias('a').join(salary_df.alias('b'), col('a.Mgrid')==col('b.EmpId'), 'left')\
    .select(col('a.EmpId'),col('a.Empname'),col('a.Mgrid'),col('b.EmpName').alias('MgrName'),col('a.deptid'),\
            col('a.salary'),col('a.sal_year'),col('a.sal_month'))

salary_df.show()

data2 = [(1,"IT"),(2,"HR")]
schema2 = ["deptid","deptname"]
dept_df = spark.createDataFrame(data2,schema2)
dept_df.show()

join_df = salary_df.join(dept_df, salary_df.deptid==dept_df.deptid,'left').drop(dept_df.deptid)
result_df1 = join_df.select('deptname','MgrName','Empname','sal_year','sal_month','salary')
result_df1 = result_df1.groupBy('deptname','MgrName','Empname','sal_year','sal_month').agg(sum('salary').alias('Month_salary'))
result_df1.show()



