import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

""" FInding Top 3 rated movies """

spark = SparkSession.builder.appName('Top3Movie').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('error')

movies = [(1,'Movie_A'),(2,'Movie_B'),(3,'Movie_C'),(4,'Movie_D'),(5,'Movie_E')]
mv_cols = 'Movie_ID int, Movie_Name string'

movies_df = spark.createDataFrame(movies, mv_cols)
movies_df.show()

ratings = [(1,101,4.5),(1,102,4.0),(2,103,5.0),
             (2,104,3.5),(3,105,4.0),(3,106,4.0),
             (4,107,3.0),(5,108,2.5),(5,109,3.0)]
rat_cols = 'Movie_ID int, User_ID int,Rating float'

rating_df = spark.createDataFrame(ratings, rat_cols)
rating_df.show()

avrg_rating = rating_df.groupBy('Movie_ID').agg(avg('Rating').alias('avrg_rating'))
avrg_rating.show()

top_3_movies = avrg_rating.join(movies_df, 'Movie_ID', 'inner').orderBy('avrg_rating', ascending=False).limit(3)
top_3_movies.show()