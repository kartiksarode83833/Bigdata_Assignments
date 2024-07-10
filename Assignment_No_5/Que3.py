# Count number of movie ratings per month using sql query (using temp views).

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

filepath = '/home/sunbeam/DBDA/BigDataTech/data/movies/ratings.csv'

spark = SparkSession.builder.getOrCreate()

ratings = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('sep', ',') \
    .csv(filepath)

ratings.show()

ratings.createOrReplaceTempView('v_ratings')


result = spark.sql("SELECT MONTH(FROM_UNIXTIME(timestamp)) AS MONTH, COUNT(rating) AS cnt FROM v_ratings GROUP BY MONTH(FROM_UNIXTIME(timestamp))")

result.show()


# output

# +-----+-----+
# |MONTH|  cnt|
# +-----+-----+
# |   12| 9693|
# |    1| 7695|
# |    6| 8869|
# |    3| 7243|
# |    5| 8416|
# |    9| 6029|
# |    4| 9388|
# |    8| 8640|
# |    7| 7653|
# |   10| 8126|
# |   11|11331|
# |    2| 6921|
# +-----+-----+

