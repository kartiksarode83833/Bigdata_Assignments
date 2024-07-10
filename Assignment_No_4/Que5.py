# 5. Movie recommendation using Spark dataframes.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession.builder
         .config("spark.driver.memory", "4g")
         .getOrCreate())

spark.sparkContext.setCheckpointDir('/home/sunbeam/DBDA/BigDataTech/spark/checkpoint/movie_recom')

movies_path = "/home/sunbeam/DBDA/BigDataTech/data/movies/movies.csv"
ratings_path = "/home/sunbeam/DBDA/BigDataTech/data/movies/ratings.csv"

movies1 = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(movies_path) \
    .drop('genres')\
    .withColumnRenamed('movieId','m1_id')\
    .withColumnRenamed('title','m1_title')

movies2 = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(movies_path) \
    .drop('genres')\
    .withColumnRenamed('movieId','m2_id')\
    .withColumnRenamed('title','m2_title')
# movies.show()

ratings_df1 = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(ratings_path) \
    .withColumnsRenamed({'movieId': 'm1', 'rating': 'r1'})

ratings_df2 = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(ratings_path) \
    .withColumnsRenamed({'movieId': 'm2', 'rating': 'r2'})

rating_matrix = ratings_df1 \
    .join(ratings_df2, on="userId", how="inner") \
    .drop('userId') \
    .drop('timestamp') \
 \
# rating_matrix.show()

corr = rating_matrix \
    .groupBy('m1', 'm2') \
    .agg(corr('r1', 'r2').alias('cor'), count('m1').alias('cnt')) \
    .where('cor is NOT NULL') \
    .select('m1', 'm2', 'cnt', 'cor') \
 \
# corr.show()

results = corr \
    .join(movies1, movies1.m1_id == corr.m1, how='inner') \
    .join(movies2, movies2.m2_id == corr.m2, how='inner') \
    .select('m1_id','m1_title','m2_id','m2_title','cnt','cor')

# results.show()
results.checkpoint(eager=True)

recomm = results\
    .select('*')\
    .where('(m1_id = 89745 OR m2_id = 89745) AND cnt > 10 AND cor > 0.7')

recomm.show(truncate=False)

spark.stop()
