# 4. Count number of movie ratings per year.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

filepath = '/home/sunbeam/DBDA/BigDataTech/data/movies/ratings.csv'

spark = SparkSession.builder.getOrCreate()

ratings = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('sep', ',') \
    .csv(filepath)

# ratings.show()

result = ratings \
    .groupBy(year(from_unixtime('timestamp')).alias('year')) \
    .count()\
    .select('year', 'count')

result.show(n=50,truncate=False)

spark.stop()

# output:
# +----+-----+
# |year|count|
# +----+-----+
# |2003|4463 |
# |2007|1548 |
# |2015|6610 |
# |2006|7493 |
# |2013|1969 |
# |1997|3294 |
# |2014|2224 |
# |2004|4658 |
# |1996|6239 |
# |1998|1825 |
# |2012|3850 |
# |2009|3432 |
# |2016|6225 |
# |1995|3    |
# |2001|4658 |
# |2005|7161 |
# |2000|13869|
# |2010|2520 |
# |2011|4449 |
# |2008|3676 |
# |1999|5901 |
# |2002|3937 |
# +----+-----+
