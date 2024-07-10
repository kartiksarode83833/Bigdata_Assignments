# Implement movie recommendation system using temp views.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

rating_path = "/home/sunbeam/DBDA/BigDataTech/data/movies/ratings.csv"
movies_path = "/home/sunbeam/DBDA/BigDataTech/data/movies/movies.csv"

spark = SparkSession.builder \
    .appName('movie_recommender') \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

ratings = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(rating_path)

movies = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(movies_path)

movies.createOrReplaceTempView("v_movies")
# ratings.show()
# ratings.printSchema()

ratings.createOrReplaceTempView('v_ratings')

movie_pairs = spark.sql(
    "SELECT r1.movieId m1_id, r1.rating m1_rating, r2.movieId m2_id, r2.rating m2_rating FROM v_ratings r1 "
    "INNER JOIN v_ratings r2 ON r1.userId = r2.userId "
    "WHERE r1.movieId < r2.movieId")

# movie_pairs.show(n=10)

movie_pairs.createOrReplaceTempView('v_pairs')

movie_corr = spark.sql(
    "SELECT m1_id, m2_id, count(m1_id) AS cnt, corr(m1_rating, m2_rating) AS cor FROM v_pairs GROUP BY m1_id, m2_id HAVING corr(m1_rating, m2_rating) IS NOT NULL")
movie_corr.show()

movie_corr.createOrReplaceTempView('v_corr')

recom = spark.sql("SELECT cb.m1_id, mv1.title, cb.m2_id, mv2.title, cb.cnt, cb.cor FROM v_corr cb "
                  "INNER JOIN v_movies mv1 ON cb.m1_id = mv1.movieId "
                  "INNER JOIN v_movies mv2 ON cb.m2_id = mv2.movieId ")

# recom.show(truncate=False, n=10)
recom.createOrReplaceTempView('v_recom')

avengers_recom = spark.sql("SELECT * FROM v_recom WHERE (m1_id = 89745 OR m2_id = 89745) AND cnt > 10 AND cor > 0.7")
avengers_recom.show(truncate=False)

spark.stop()
