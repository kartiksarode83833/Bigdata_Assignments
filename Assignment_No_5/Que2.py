# 2. Read ncdc data from mysql table and print average temperature per
# year in DESC order.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('ncdc_analysis') \
    .getOrCreate()

dbUrl = 'jdbc:mysql://localhost:3306/classwork_db'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'root'
dbPassword = 'manager'
dbTable = 'ncdc_data'

ncdc_data = spark.read\
    .option('url',dbUrl)\
    .option('driver',dbDriver)\
    .option('user',dbUser)\
    .option('password',dbPassword)\
    .option('dbtable',dbTable)\
    .format('jdbc')\
    .load()

ncdc_data.createOrReplaceTempView('v_ncdc')


avg_temp = spark.sql("SELECT year, AVG(temp) AS avg_temp FROM v_ncdc GROUP BY year ORDER BY avg_temp DESC")

avg_temp.show()

spark.stop()

# output:

# +----+------------------+
# |year|          avg_temp|
# +----+------------------+
# |1903|48.241744739671326|
# |1906|  47.0834855681403|
# |1901|   46.698507007922|
# |1920|43.508667830133795|
# |1905|  43.3322664228014|
# |1910|35.558665794637015|
# |1904| 33.32224247948952|
# |1907| 31.76414576084966|
# |1918|31.351966122202057|
# |1911|30.719045120671563|
# |1913|29.958786491127647|
# |1914|29.817932296431838|
# |1908| 28.80607441154138|
# |1919|27.605149653640048|
# |1909|26.579429329794294|
# |1917|22.895140080045742|
# |1902|21.659558263518658|
# |1916| 21.42393787117405|
# |1912|16.801145236855803|
# |1915| 5.098548073625243|
# +----+------------------+
