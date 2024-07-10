# 2. Find max sal per dept per job in emp.csv file.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

emp_path = '/home/sunbeam/DBDA/BigDataTech/data/emp.csv'
dept_path = '/home/sunbeam/DBDA/BigDataTech/data/dept.csv'

spark = SparkSession.builder.getOrCreate()

emp = spark.read \
    .option('header', 'false') \
    .option('inferSchema', 'true') \
    .csv(emp_path) \
    .withColumnsRenamed(
    {'_c0': 'empno', '_c1': 'ename', '_c2': 'job', '_c3': 'mgr', '_c4': 'hire', '_c5': 'sal', '_c6': 'comm',
     '_c7': 'deptno'})

# emp.printSchema()
# emp.show(truncate=False)

dept = spark.read \
    .option('header', 'false') \
    .option('inferSchema', 'true') \
    .csv(dept_path) \
    .withColumnsRenamed({'_c0': 'deptno', '_c1': 'dname', '_c2': 'location'})

# dept.printSchema()
# dept.show()

result = emp \
    .groupBy('deptno', 'job') \
    .max('sal') \
    .alias('sal_max') \
    .join(dept, on='deptno', how='inner') \
    .select('dname', 'job', 'sal_max.max(sal)')

result.show(truncate=False)
spark.stop()
# output:
# +----------+---------+--------+
# |dname     |job      |max(sal)|
# +----------+---------+--------+
# |RESEARCH  |ANALYST  |3000.0  |
# |RESEARCH  |MANAGER  |2975.0  |
# |SALES     |MANAGER  |2850.0  |
# |SALES     |SALESMAN |1600.0  |
# |SALES     |CLERK    |950.0   |
# |RESEARCH  |CLERK    |1100.0  |
# |ACCOUNTING|PRESIDENT|5000.0  |
# |ACCOUNTING|CLERK    |1300.0  |
# |ACCOUNTING|MANAGER  |2450.0  |
# +----------+---------+--------+
