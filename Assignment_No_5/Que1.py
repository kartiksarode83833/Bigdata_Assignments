# 1. Clean NCDC data and write year, temperature and quality data into mysql table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

filepath = "/home/sunbeam/DBDA/BigDataTech/data/ncdc/"

spark = SparkSession.builder\
         .appName('ncdc_data_cleaning') \
         .config('spark.sql.warehouse.dir', 'file:///home/sunbeam/bigdata/spark-warehouse') \
         .enableHiveSupport() \
         .getOrCreate()

ncdc = spark.read.text(filepath)

regex = r'^.{15}([0-9]{4}).{68}([-\+][0-9]{4})([0-9]).*$'

temps = ncdc\
    .select(regexp_extract('value',regex,1).alias('year'),
            regexp_extract('value',regex,2).alias('temp'),
            regexp_extract('value',regex,3).alias('quality'))\
    .where('quality IN (0,1,2,4,5,9) AND temp != 9999')

# temps.show()

# temps.write\
#     .mode('append')\
#     .saveAsTable('ncdc_table_clean')

dbUrl = 'jdbc:mysql://localhost:3306/classwork_db'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'root'
dbPassword = 'manager'
dbTable = 'ncdc_data'

temps.write\
    .option('url',dbUrl)\
    .option('driver',dbDriver)\
    .option('user',dbUser)\
    .option('password',dbPassword)\
    .option('dbtable',dbTable)\
    .mode('APPEND')\
    .format('jdbc')\
    .save()

spark.stop()
