# 1. Wordcount using Spark Dataframes and top 10 words (except stopwords).

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

filepath = '/home/sunbeam/hadoop-3.3.2/LICENSE.txt'

word_file = spark.read \
    .option('inferSchema', 'true') \
    .option('sep', ' ') \
    .text(filepath)

# word_file.printSchema()
# word_file.show(truncate=False)

result = word_file.select(explode(split(lower('value'), '[^a-z0-9]')).alias('words')) \
    .groupBy('words').count() \
    .where("words NOT IN (' ','','the','of','or','and','to','any','you','for','in','by','a','an', 'that', 'this', "
           "'under', 'over','as','shall','1','your','such','not')") \
    .orderBy(desc('count')) \
    .limit(10)

result.show(truncate=False, n=20)


spark.stop()

# output:

# +-------+-----+
# |words  |count|
# +-------+-----+
# |hadoop |89   |
# |hdfs   |40   |
# |license|38   |
# |src    |36   |
# |main   |35   |
# |work   |34   |
# |project|31   |
# |yarn   |26   |
# |native |20   |
# |works  |19   |
# +-------+-----+
