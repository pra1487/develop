import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkFiles
from urllib.request import urlopen


spark  = SparkSession.builder.appName('AWS pipe line').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('Error')

#webapi data load

url = "https://randomuser.me/api/0.8/?results=200"
jd = urlopen(url).read().decode('utf-8')
rdd = sc.parallelize([jd])
df = spark.read.json(rdd)
df.show()

df1 = df.withColumn('results', explode(col('results')))
df1.show(4)
df1.printSchema()

web_df = df1.select(col('nationality').alias('nationality'),
                    col('results.user.cell').alias('cell'),
                    col('results.user.dob').alias('dob'),
                    col('results.user.email').alias('email'),
                    col('results.user.gender').alias('gender'),
                    col('results.user.location.city').alias('city'),
                    col('results.user.location.state').alias('state'),
                    col('results.user.location.street').alias('street'),
                    col('results.user.location.zip').alias('zip'),
                    col('results.user.md5').alias('md5'),
                    col('results.user.name.first').alias('fname'),
                    col('results.user.name.last').alias('lname'),
                    col('results.user.name.title').alias('title'),
                    col('results.user.password').alias('password'),
                    col('results.user.phone').alias('phone'),
                    col('results.user.picture.large').alias('large'),
                    col('results.user.picture.medium').alias('medium'),
                    col('results.user.picture.thumbnail').alias('thumbnail'),
                    col('results.user.registered').alias('registered'),
                    col('results.user.salt').alias('salt'),
                    col('results.user.sha1').alias('sha1'),
                    col('results.user.sha256').alias('sha256'),
                    col('results.user.username').alias('username'),
                    col('seed'),
                    col('version'))
#web_df.show(10)

web_df.write.mode('overwrite').format('csv').option('header',True).save('D://data/Writedata/web_api/csv/')

