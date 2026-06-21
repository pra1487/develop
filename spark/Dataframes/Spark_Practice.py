from pkg_resources import non_empty_lines

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('practice').master('local[*]').getOrCreate()

