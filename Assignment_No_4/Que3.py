# 3. Find deptwise total sal from emp.csv and dept.csv.
# Print dname and total sal.

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
    .groupBy('deptno') \
    .sum('sal') \
    .alias('sum_sal') \
    .join(dept, on='deptno', how='inner') \
    .select('dname', 'sum_sal.sum(sal)')

result.show()

spark.stop()
# output:
#
# +----------+--------+
# |     dname|sum(sal)|
# +----------+--------+
# |  RESEARCH| 10875.0|
# |ACCOUNTING|  8750.0|
# |     SALES|  9400.0|
# +----------+--------+
