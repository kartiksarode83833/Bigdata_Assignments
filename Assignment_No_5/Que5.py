# Load Fire Service Calls with pre-defined schema. Repeat all 10 Hive assignments on that dataset.
# Do the assignments using Dataframe syntax.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

filepath = "/home/sunbeam/DBDA/BigDataTech/data/Fire_Department_Calls_for_Service.csv"

spark = SparkSession.builder \
    .appName('fire_data_analysis') \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

schema1 = "call_no BIGINT,\
    unit_id STRING,\
    incident_no BIGINT,\
    call_type STRING,\
    call_date STRING,\
    watch_date STRING,\
    received_dttm STRING,\
    entry_dttm STRING,\
    dispatch_dttm STRING,\
    response_dttm STRING,\
    onscene_dttm STRING,\
    transport_dttm STRING,\
    hospital_dttm STRING,\
    call_final_disp STRING,\
    available_dttm STRING,\
    address STRING,\
    city STRING,\
    zipcode INT,\
    battalion STRING,\
    station_area STRING,\
    box INT,\
    original_priority SMALLINT,\
    priority SMALLINT,\
    final_priority SMALLINT,\
    ALS_unit BOOLEAN,\
    call_type_group STRING,\
    no_alarms SMALLINT,\
    unit_type STRING,\
    unit_sequence SMALLINT,\
    fire_prevention_dist SMALLINT,\
    supervisor_dist SMALLINT,\
    neighborhoods STRING,\
    rowid STRING,\
    case_location STRING,\
    data_as_of STRING,\
    data_loaded_at STRING,\
    analysis_neighbourhoods SMALLINT"

fire_data = spark.read \
    .option('header', 'true') \
    .option('nullValue', 'NULL') \
    .schema(schema1) \
    .csv(filepath)


fire_data = fire_data.withColumn('response_ts',to_timestamp(fire_data['response_dttm'],'MM/dd/yyyy hh:mm:ss a'))
fire_data = fire_data.withColumn('received_ts',to_timestamp(fire_data['received_dttm'],'MM/dd/yyyy hh:mm:ss a'))
fire_data = fire_data.withColumn('call_date_ts',to_timestamp(fire_data['call_date'],'MM/dd/yyyy'))
fire_data = fire_data.withColumn('response_time',(unix_timestamp('response_ts')-unix_timestamp('received_ts'))/60)
fire_data = fire_data.withColumn('call_year',year(to_timestamp(fire_data['call_date'],'MM/dd/yyyy')))
# fire_data.printSchema()
# fire_data.show(truncate=False)

# 1. How many distinct types of calls were made to the fire department?

dis_calls = fire_data \
    .select(countDistinct('call_type').alias('call_type_cnt'))

# dis_calls.show(truncate=False, n=50)

# +-------------+
# |call_type_cnt|
# +-------------+
# |33           |
# +-------------+

# 2. What are distinct types of calls made to the fire department?

dis_calls = fire_data \
    .select('call_type')\
    .distinct()

# dis_calls.show(truncate=False, n=50)

# +--------------------------------------------+
# |call_type                                   |
# +--------------------------------------------+
# |Elevator / Escalator Rescue                 |
# |Marine Fire                                 |
# |Aircraft Emergency                          |
# |Confined Space / Structure Collapse         |
# |Structure Fire / Smoke in Building          |
# |Administrative                              |
# |Alarms                                      |
# |Odor (Strange / Unknown)                    |
# |Citizen Assist / Service Call               |
# |HazMat                                      |
# |Watercraft in Distress                      |
# |Explosion                                   |
# |Oil Spill                                   |
# |Vehicle Fire                                |
# |Suspicious Package                          |
# |Extrication / Entrapped (Machinery, Vehicle)|
# |Other                                       |
# |Outside Fire                                |
# |Traffic Collision                           |
# |Assist Police                               |
# |Gas Leak (Natural and LP Gases)             |
# |Water Rescue                                |
# |Electrical Hazard                           |
# |High Angle Rescue                           |
# |Structure Fire                              |
# |Industrial Accidents                        |
# |Medical Incident                            |
# |Mutual Aid / Assist Outside Agency          |
# |Fuel Spill                                  |
# |Smoke Investigation (Outside)               |
# |Train / Rail Incident                       |
# |Lightning Strike (Investigation)            |
# |Train / Rail Fire                           |
# +--------------------------------------------+


# 3. Find out all responses for delayed times greater than 5 mins?

dis_calls = fire_data \
    .where('response_time > 5.0')\
    .select("call_final_disp")\
    .distinct()

# dis_calls.show(truncate=False)

# +--------------------------+
# |call_final_disp           |
# +--------------------------+
# |Medical Examiner          |
# |Duplicate                 |
# |CHP                       |
# |No Merit                  |
# |Patient Declined Transport|
# |Gone on Arrival           |
# |Unable to Locate          |
# |Cancelled                 |
# |SFPD                      |
# |Other                     |
# |Against Medical Advice    |
# |Code 3 Transport          |
# |Fire                      |
# |Code 2 Transport          |
# |Multi-casualty Incident   |
# +--------------------------+


# 4. What were the most common call types?

dis_calls = fire_data\
        .groupBy('call_type')\
        .count()\
        .orderBy('count',ascending=False)\
        .select('call_type','count')\
        .limit(10)

# dis_calls.show(truncate=False, n=25)

# +-------------------------------+-------+
# |call_type                      |count  |
# +-------------------------------+-------+
# |Medical Incident               |4247943|
# |Alarms                         |720968 |
# |Structure Fire                 |714873 |
# |Traffic Collision              |259541 |
# |Other                          |110855 |
# |Citizen Assist / Service Call  |96222  |
# |Outside Fire                   |85967  |
# |Water Rescue                   |34061  |
# |Gas Leak (Natural and LP Gases)|30484  |
# |Vehicle Fire                   |28378  |
# +-------------------------------+-------+


# 5. What zip codes accounted for the most common calls?


dis_calls = fire_data\
        .groupBy('zipcode','call_type')\
        .count()\
        .select('zipcode','call_type','count')\
        .orderBy('count',ascending=False)\
        .limit(10)

# dis_calls.show()

# +-------+----------------+------+
# |zipcode|       call_type| count|
# +-------+----------------+------+
# |  94102|Medical Incident|616021|
# |  94103|Medical Incident|577576|
# |  94109|Medical Incident|372844|
# |  94110|Medical Incident|363740|
# |  94124|Medical Incident|218131|
# |  94112|Medical Incident|205392|
# |  94115|Medical Incident|175661|
# |  94107|Medical Incident|154910|
# |  94122|Medical Incident|152124|
# |  94133|Medical Incident|141632|
# +-------+----------------+------+


# 6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

dis_calls = fire_data\
        .select('neighborhoods')\
        .where("city = 'San Francisco' AND zipcode IN (94102, 94103)")\
        .distinct()\

# dis_calls.show(truncate=False, n=50)

# +------------------------------+
# |neighborhoods                 |
# +------------------------------+
# |Western Addition              |
# |Mission Bay                   |
# |Hayes Valley                  |
# |Financial District/South Beach|
# |Nob Hill                      |
# |Mission                       |
# |Tenderloin                    |
# |Castro/Upper Market           |
# |South of Market               |
# |Potrero Hill                  |
# +------------------------------+



# 7. What was the sum of all calls, average, min, and max of the call response times?

calls = fire_data\
    .agg(sum('response_time').alias('sum'), avg('response_time').alias('avg'), min('response_time').alias('min'), max('response_time').alias('max'))\
    .select('sum','avg','min','max')

# calls.show()

# +--------------------+-----------------+------------------+------------------+
# |                 sum|              avg|               min|               max|
# +--------------------+-----------------+------------------+------------------+
# |2.4017975983333357E7|4.001277448558071|-713.4666666666667|2465.2833333333333|
# +--------------------+-----------------+------------------+------------------+

# 8. How many distinct years of data are in the CSV le?

years_cnt = fire_data\
    .select(countDistinct(year('call_date_ts')).alias('year_cnt'))

# years_cnt.show()

# +--------+
# |year_cnt|
# +--------+
# |      24|
# +--------+


# 9. What week of the year in 2018 had the most re calls?

week = fire_data\
    .groupBy(year('call_date_ts').alias('year'),weekofyear('call_date_ts').alias('week'))\
    .count()\
    .where("year = 2018")\
    .orderBy('count',ascending=False)\
    .limit(1)

# week.show()

# +----+----+-----+
# |year|week|count|
# +----+----+-----+
# |2018|   1| 7545|
# +---------+-----+


# 10. What neighborhoods in San Francisco had the worst response time in 2018?

result_df = fire_data\
            .where((col('city') == 'San Francisco') & (year('call_date_ts') == 2018)) \
              .select('neighborhoods', 'response_time') \
              .distinct() \
              .orderBy(col('response_time').desc()) \
              .limit(3)

result_df.show(truncate=False)

# +---------------------+-----------------+
# |neighborhoods        |response_time    |
# +---------------------+-----------------+
# |West of Twin Peaks   |754.0833333333334|
# |Chinatown            |734.8666666666667|
# |Bayview Hunters Point|715.7666666666667|
# +---------------------+-----------------+
