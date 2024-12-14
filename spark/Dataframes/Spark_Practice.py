import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from urllib.request import urlopen

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()

from datetime import date

cycl_id = "".join(str(date.today()).split('-'))+'000000'
print(f'cycl_id: {cycl_id}')