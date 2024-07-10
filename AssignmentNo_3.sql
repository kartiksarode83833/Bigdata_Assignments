1. Execute following queries on MySQL emp database using Recursive CTEs (not supported in Hive 3.x).

1. Find years in range 1975 to 1985, where no emps were hired.

WITH RECURSIVE years(n) AS (
    (SELECT 1975)
    UNION
    (SELECT n+1 FROM years WHERE n < 1985)
)
SELECT n AS year FROM years WHERE n NOT IN (SELECT YEAR(hire) FROM emp);

+------+
| year |
+------+
| 1975 |
| 1976 |
| 1977 |
| 1978 |
| 1979 |
| 1984 |
| 1985 |
+------+



2. Display emps with their level in emp hierarchy. Level employee is Level of his manager + 1.

WITH RECURSIVE emp_levels(empno, ename, lvl) AS (
    (SELECT empno, ename, 1 FROM emp WHERE mgr IS NULL)
    UNION ALL
    (SELECT e.empno, e.ename, l.lvl + 1 FROM emp e
    INNER JOIN emp_levels l ON e.mgr = l.empno)
)
SELECT empno, ename, lvl FROM emp_levels;

+-------+--------+------+
| empno | ename  | lvl  |
+-------+--------+------+
|  7839 | KING   |    1 |
|  7566 | JONES  |    2 |
|  7698 | BLAKE  |    2 |
|  7782 | CLARK  |    2 |
|  7499 | ALLEN  |    3 |
|  7521 | WARD   |    3 |
|  7654 | MARTIN |    3 |
|  7788 | SCOTT  |    3 |
|  7844 | TURNER |    3 |
|  7900 | JAMES  |    3 |
|  7902 | FORD   |    3 |
|  7934 | MILLER |    3 |
|  7369 | SMITH  |    4 |
|  7876 | ADAMS  |    4 |
+-------+--------+------+



3. Create a "newemp" table with foreign constraints enabled for "mgr" column. Also enable DELETE ON CASCADE for the same. Insert data into the
table from emp table. Hint: You need to insert data levelwise to avoid FK constraint error.

CREATE TABLE newemp(
    empno INT,

)



4. From "newemp" table, delete employee KING. What is result?



---------------------------------------------------------------------------------------------------------------------------------------------


2. Implement movie recommendation in python/java + hive.


## PYTHON:
from pyhive import hive

# hive config
host_name = 'localhost'
port = 10000
user = 'sunbeam'
password = 'manager'
db_name = 'classwork'

conn = hive.Connection(host=host_name,port=port,username=user,password=password,database=db_name,auth='CUSTOM')
cur = conn.cursor()

movie_id = int(input("Enter movie id to generate its recommendations: "))
sql = (f"SELECT m1_title, m2_title FROM movie_recom WHERE (m1_id = {movie_id} OR m2_id = {movie_id}) AND cnt > 10 AND "
       f"cor > 0.7")
cur.execute(sql)

result = cur.fetchall()

for row in result:
    print(row)

conn.close()

terminal --

Enter movie id to generate its recommendations: 89745

These are the recommendations: 

('Independence Day (a.k.a. ID4) (1996)', 'Avengers, The (2012)')
('Lock, Stock & Two Smoking Barrels (1998)', 'Avengers, The (2012)')
('Mission: Impossible II (2000)', 'Avengers, The (2012)')
('Equilibrium (2002)', 'Avengers, The (2012)')
('Captain America: The First Avenger (2011)', 'Avengers, The (2012)')
('Avengers, The (2012)', 'Sherlock Holmes: A Game of Shadows (2011)')
('Avengers, The (2012)', 'Star Trek Into Darkness (2013)')
('Jumanji (1995)', 'Avengers, The (2012)')
('Saving Private Ryan (1998)', 'Avengers, The (2012)')
('Mummy, The (1999)', 'Avengers, The (2012)')
('Harry Potter and the Order of the Phoenix (2007)', 'Avengers, The (2012)')
('Thor (2011)', 'Avengers, The (2012)')
('Avengers, The (2012)', 'Men in Black III (M.III.B.) (M.I.B.³) (2012)')
('Avengers, The (2012)', 'Avengers: Age of Ultron (2015)')

Process finished with exit code 0


-------------------------------------------------------------------------------------------------------------

3. Create ORC table emp_job_part to partition emp data jobwise. 
Upload emp data dynamically into these partitions.

CREATE TABLE emp_job_part 
(
    empno INT,
    ename STRING,
    mgr INT,
    hire_date Date,
    salary DOUBLE,
    comm DOUBLE,
    deptno INT
)
PARTITIONED BY (job STRING)
STORED AS ORC
TBLPROPERTIES('transactional'='true');

INSERT INTO emp_job_part PARTITION(job)
SELECT empno, ename, mgr, hire_date, salary, comm, deptno, job FROM emp_staging;



4. Create ORC table emp_job_dept_part to partition emp data jobwise and deptwise. Also divide them into two buckets by empno. Upload emp data
dynamically into these partitions.

CREATE TABLE emp_job_dept_part(
    empno INT,
    ename STRING,
    mgr INT,
    hire_date Date,
    salary DOUBLE,
    comm DOUBLE
)
PARTITIONED BY (job STRING, deptno INT)
CLUSTERED BY (empno) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES('transactional'='true');

INSERT INTO emp_job_dept_part PARTITION(job, deptno)
SELECT empno, ename, mgr, hire_date, salary, comm, job, deptno FROM emp_staging;


5. Load Fire data into Hive in a staging table " re_staging".

hadoop fs -mkdir -p /user/$USER/fire_dept/input

hadoop fs -put /home/sunbeam/DBDA/BigDataTech/data/Fire_Department_Calls_for_Service.csv /user/$USER/fire_dept/input

hadoop fs -ls /user/$USER/fire_dept/input

CREATE EXTERNAL TABLE re_staging(
    call_no BIGINT,
    unit_id STRING,
    incident_no BIGINT,
    call_type STRING,
    call_date STRING,
    watch_date STRING,
    received_dttm STRING,
    entry_dttm STRING,
    dispatch_dttm STRING,
    response_dttm STRING,
    onscene_dttm STRING,
    transport_dttm STRING,
    hospital_dttm STRING,
    call_final_disp STRING,
    available_dttm STRING,
    address STRING,
    city STRING,
    zipcode INT,
    battalion STRING,
    station_area STRING,
    box INT,
    original_priority SMALLINT,
    priority SMALLINT,
    final_priority SMALLINT,
    ALS_unit BOOLEAN,
    call_type_group STRING,
    no_alarms SMALLINT,
    unit_type STRING,
    unit_sequence SMALLINT,
    fire_prevention_dist SMALLINT,
    supervisor_dist SMALLINT,
    neighborhoods STRING,
    rowid STRING,
    case_location STRING,
    data_as_of STRING,
    data_loaded_at STRING,
    analysis_neighbourhoods SMALLINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    'separatorChar'=',',
    'quoteChar'='"',
    'escapeChar'='\\'
)
STORED AS TEXTFILE
LOCATION '/user/sunbeam/fire_dept/input'
TBLPROPERTIES('skip.header.line.count'='1');

SELECT * FROM re_staging LIMIT 20,5;
+---------------------+---------------------+-------------------------+-----------------------+-----------------------+------------------------+---------------------------+-------------------------+---------------------------+---------------------------+--------------------------+----------------------------+---------------------------+-----------------------------+----------------------------+---------------------------+------------------+---------------------+-----------------------+--------------------------+-----------------+-------------------------------+----------------------+----------------------------+----------------------+-----------------------------+-----------------------+-----------------------+---------------------------+----------------------------------+-----------------------------+---------------------------+-------------------+--------------------------------------------+------------------------+----------------------------+-------------------------------------+
| re_staging.call_no  | re_staging.unit_id  | re_staging.incident_no  | re_staging.call_type  | re_staging.call_date  | re_staging.watch_date  | re_staging.received_dttm  |  re_staging.entry_dttm  | re_staging.dispatch_dttm  | re_staging.response_dttm  | re_staging.onscene_dttm  | re_staging.transport_dttm  | re_staging.hospital_dttm  | re_staging.call_final_disp  | re_staging.available_dttm  |    re_staging.address     | re_staging.city  | re_staging.zipcode  | re_staging.battalion  | re_staging.station_area  | re_staging.box  | re_staging.original_priority  | re_staging.priority  | re_staging.final_priority  | re_staging.als_unit  | re_staging.call_type_group  | re_staging.no_alarms  | re_staging.unit_type  | re_staging.unit_sequence  | re_staging.fire_prevention_dist  | re_staging.supervisor_dist  | re_staging.neighborhoods  | re_staging.rowid  |          re_staging.case_location          | re_staging.data_as_of  | re_staging.data_loaded_at  | re_staging.analysis_neighbourhoods  |
+---------------------+---------------------+-------------------------+-----------------------+-----------------------+------------------------+---------------------------+-------------------------+---------------------------+---------------------------+--------------------------+----------------------------+---------------------------+-----------------------------+----------------------------+---------------------------+------------------+---------------------+-----------------------+--------------------------+-----------------+-------------------------------+----------------------+----------------------------+----------------------+-----------------------------+-----------------------+-----------------------+---------------------------+----------------------------------+-----------------------------+---------------------------+-------------------+--------------------------------------------+------------------------+----------------------------+-------------------------------------+
| 080960182           | E03                 | 08029452                | Medical Incident      | 04/05/2008            | 04/05/2008             | 04/05/2008 01:05:34 PM    | 04/05/2008 01:06:35 PM  | 04/05/2008 01:06:49 PM    | 04/05/2008 01:07:55 PM    |                          |                            |                           | Other                       |                            | 1000 Block of MARKET ST   | SF               | 94102               | B03                   | 01                       | 1454            | 3                             | 3                    | 3                          | false                |                             | 1                     | ENGINE                | 1                         | 3                                | 6                           | Tenderloin                | 080960182-E03     | POINT (-122.411058175414 37.781761594075)  |                        | 11/30/2023 09:35:04 AM     | 36                                  |
| 080020323           | B01                 | 08000704                | Alarms                | 01/02/2008            | 01/02/2008             | 01/02/2008 06:09:26 PM    | 01/02/2008 06:11:58 PM  | 01/02/2008 06:12:26 PM    | 01/02/2008 06:14:40 PM    |                          |                            |                           | Other                       | 01/02/2008 06:15:32 PM     | 200 Block of BEACH ST     | SF               | 94133               | B01                   | 28                       | 1344            | 3                             | 3                    | 3                          | false                |                             | 1                     | CHIEF                 | 2                         | 1                                | 3                           | North Beach               | 080020323-B01     | POINT (-122.413163175978 37.807640748414)  |                        | 11/30/2023 09:35:04 AM     | 23                                  |
| 080240092           | E06                 | 08007531                | Alarms                | 01/24/2008            | 01/24/2008             | 01/24/2008 08:39:38 AM    | 01/24/2008 08:40:59 AM  | 01/24/2008 08:41:04 AM    |                           | 01/24/2008 08:42:09 AM   |                            |                           | Other                       | 01/24/2008 08:53:32 AM     | 300 Block of VALENCIA ST  | SF               | 94103               | B02                   | 06                       | 5226            | 3                             | 3                    | 3                          | true                 |                             | 1                     | ENGINE                | 1                         | 2                                | 8                           | Mission                   | 080240092-E06     | POINT (-122.422158110393 37.767002028648)  |                        | 11/30/2023 09:35:04 AM     | 20                                  |
| 080390372           | T18                 | 08012422                | Traffic Collision     | 02/08/2008            | 02/08/2008             | 02/08/2008 06:20:26 PM    | 02/08/2008 06:21:24 PM  | 02/08/2008 06:21:40 PM    |                           |                          |                            |                           | Other                       |                            | 25TH AV/ORTEGA ST         | SF               | 94116               | B08                   | 18                       | 7445            | 3                             | 3                    | 3                          | false                |                             | 1                     | TRUCK                 | 3                         | 8                                | 4                           | Sunset/Parkside           | 080390372-T18     | POINT (-122.482769583257 37.752092895593)  |                        | 11/30/2023 09:35:04 AM     | 35                                  |
| 081010042           | 79                  | 08030773                | Medical Incident      | 04/10/2008            | 04/09/2008             | 04/10/2008 06:16:35 AM    | 04/10/2008 06:18:28 AM  | 04/10/2008 06:22:16 AM    |                           |                          |                            |                           | Other                       |                            | 900 Block of SUTTER ST    | SF               | 94109               | B04                   | 03                       | 1557            | 1                             | 1                    | 2                          | true                 |                             | 1                     | MEDIC                 | 2                         | 4                                | 3                           | Nob Hill                  | 081010042-79      | POINT (-122.415995198292 37.788228686156)  |                        | 11/30/2023 09:35:04 AM     | 21                                  |
+---------------------+---------------------+-------------------------+-----------------------+-----------------------+------------------------+---------------------------+-------------------------+---------------------------+---------------------------+--------------------------+----------------------------+---------------------------+-----------------------------+----------------------------+---------------------------+------------------+---------------------+-----------------------+--------------------------+-----------------+-------------------------------+----------------------+----------------------------+----------------------+-----------------------------+-----------------------+-----------------------+---------------------------+----------------------------------+-----------------------------+---------------------------+-------------------+--------------------------------------------+------------------------+----------------------------+-------------------------------------+

6. Create a transactional ORC table " re_data" with appropriate data types partitioned by city and buckted by call number into 4 buckets. Load data from
staging table into this table.

CREATE TABLE re_data(
    call_no BIGINT,
    unit_id STRING,
    incident_no BIGINT,
    call_type STRING,
    call_date TIMESTAMP,
    watch_date TIMESTAMP,
    received_dttm TIMESTAMP,
    entry_dttm TIMESTAMP,
    dispatch_dttm TIMESTAMP,
    response_dttm TIMESTAMP,
    onscene_dttm TIMESTAMP,
    transport_dttm TIMESTAMP,
    hospital_dttm TIMESTAMP,
    call_final_disp STRING,
    available_dttm TIMESTAMP,
    address STRING,
    zipcode INT,
    battalion STRING,
    station_area STRING,
    box INT,
    original_priority SMALLINT,
    priority SMALLINT,
    final_priority SMALLINT,
    ALS_unit BOOLEAN,
    call_type_group STRING,
    no_alarms SMALLINT,
    unit_type STRING,
    unit_sequence SMALLINT,
    fire_prevention_dist SMALLINT,
    supervisor_dist SMALLINT,
    neighborhoods STRING,
    rowid STRING,
    case_location STRING,
    data_as_of STRING,
    data_loaded_at TIMESTAMP,
    analysis_neighbourhoods SMALLINT
)
PARTITIONED BY (city STRING)
CLUSTERED BY (call_no) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES('transactional'='true');

INSERT INTO re_data PARTITION(city)
SELECT call_no, unit_id, incident_no, call_type, 
from_unixtime(unix_timestamp(call_date, 'MM/dd/yyyy')),
from_unixtime(unix_timestamp(watch_date, 'MM/dd/yyyy')),
from_unixtime(unix_timestamp(received_dttm, 'MM/dd/yyyy hh:mm:ss a')),
from_unixtime(unix_timestamp(entry_dttm, 'MM/dd/yyyy hh:mm:ss a')),
from_unixtime(unix_timestamp(dispatch_dttm, 'MM/dd/yyyy hh:mm:ss a')),
from_unixtime(unix_timestamp(response_dttm, 'MM/dd/yyyy hh:mm:ss a')),
from_unixtime(unix_timestamp(onscene_dttm, 'MM/dd/yyyy hh:mm:ss a')),
from_unixtime(unix_timestamp(transport_dttm, 'MM/dd/yyyy hh:mm:ss a')),
from_unixtime(unix_timestamp(hospital_dttm, 'MM/dd/yyyy hh:mm:ss a')),
call_final_disp,
from_unixtime(unix_timestamp(available_dttm, 'MM/dd/yyyy hh:mm:ss a')),
address, zipcode, battalion, station_area, box, original_priority, priority,
final_priority, ALS_unit, call_type_group, no_alarms, unit_type,
unit_sequence, fire_prevention_dist, supervisor_dist, neighborhoods, rowid, case_location,
data_as_of,
from_unixtime(unix_timestamp(data_loaded_at, 'MM/dd/yyyy hh:mm:ss a')),
analysis_neighbourhoods, city FROM re_staging;

SELECT * FROM re_data LIMIT 20,5;

+------------------+------------------+----------------------+-------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+-------------------------+------------------------+--------------------------+-------------------------+------------------------------+------------------+--------------------+-----------------------+--------------+----------------------------+-------------------+-------------------------+-------------------+--------------------------+--------------------+--------------------+------------------------+-------------------------------+--------------------------+------------------------+----------------+--------------------------------------------+---------------------+-------------------------+----------------------------------+---------------+
| re_data.call_no  | re_data.unit_id  | re_data.incident_no  |    re_data.call_type    |   re_data.call_date    |   re_data.watch_date   | re_data.received_dttm  |   re_data.entry_dttm   | re_data.dispatch_dttm  | re_data.response_dttm  |  re_data.onscene_dttm  | re_data.transport_dttm  | re_data.hospital_dttm  | re_data.call_final_disp  | re_data.available_dttm  |       re_data.address        | re_data.zipcode  | re_data.battalion  | re_data.station_area  | re_data.box  | re_data.original_priority  | re_data.priority  | re_data.final_priority  | re_data.als_unit  | re_data.call_type_group  | re_data.no_alarms  | re_data.unit_type  | re_data.unit_sequence  | re_data.fire_prevention_dist  | re_data.supervisor_dist  | re_data.neighborhoods  | re_data.rowid  |           re_data.case_location            | re_data.data_as_of  | re_data.data_loaded_at  | re_data.analysis_neighbourhoods  | re_data.city  |
+------------------+------------------+----------------------+-------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+-------------------------+------------------------+--------------------------+-------------------------+------------------------------+------------------+--------------------+-----------------------+--------------+----------------------------+-------------------+-------------------------+-------------------+--------------------------+--------------------+--------------------+------------------------+-------------------------------+--------------------------+------------------------+----------------+--------------------------------------------+---------------------+-------------------------+----------------------------------+---------------+
| 112560266        | T16              | 11084497             | Water Rescue            | 2011-09-13 00:00:00.0  | 2011-09-13 00:00:00.0  | 2011-09-13 16:44:56.0  | 2011-09-13 16:47:30.0  | 2011-09-13 16:49:27.0  | 2011-09-13 16:49:57.0  | 2011-09-13 16:51:14.0  | NULL                    | NULL                   | Other                    | 2011-09-13 18:04:13.0   | CALL BOX: 1 ANGEL ISLAND DR  | NULL             | B99                | 94                    | NULL         | NULL                       | NULL              | 2                       | false             |                          | 1                  | TRUCK              | 2                      | NULL                          | NULL                     | None                   | 112560266-T16  | POINT (-122.421080465833 37.854464340117)  |                     | 2023-11-30 09:35:04.0   | NULL                             | AI            |
| 112560266        | FB1              | 11084497             | Water Rescue            | 2011-09-13 00:00:00.0  | 2011-09-13 00:00:00.0  | 2011-09-13 16:44:56.0  | 2011-09-13 16:47:30.0  | 2011-09-13 16:49:27.0  | 2011-09-13 16:55:09.0  | NULL                   | NULL                    | NULL                   | Other                    | 2011-09-13 17:29:01.0   | CALL BOX: 1 ANGEL ISLAND DR  | NULL             | B99                | 94                    | NULL         | NULL                       | NULL              | 2                       | false             |                          | 1                  | SUPPORT            | 14                     | NULL                          | NULL                     | None                   | 112560266-FB1  | POINT (-122.421080465833 37.854464340117)  |                     | 2023-11-30 09:35:04.0   | NULL                             | AI            |
| 112560266        | E48              | 11084497             | Water Rescue            | 2011-09-13 00:00:00.0  | 2011-09-13 00:00:00.0  | 2011-09-13 16:44:56.0  | 2011-09-13 16:47:30.0  | 2011-09-13 16:49:27.0  | 2011-09-13 16:55:08.0  | 2011-09-13 17:01:45.0  | NULL                    | NULL                   | Other                    | 2011-09-13 17:07:19.0   | CALL BOX: 1 ANGEL ISLAND DR  | NULL             | B99                | 94                    | NULL         | NULL                       | NULL              | 2                       | true              |                          | 1                  | ENGINE             | 4                      | NULL                          | NULL                     | None                   | 112560266-E48  | POINT (-122.421080465833 37.854464340117)  |                     | 2023-11-30 09:35:04.0   | NULL                             | AI            |
| 112560266        | B03              | 11084497             | Water Rescue            | 2011-09-13 00:00:00.0  | 2011-09-13 00:00:00.0  | 2011-09-13 16:44:56.0  | 2011-09-13 16:47:30.0  | 2011-09-13 16:49:27.0  | 2011-09-13 16:54:15.0  | 2011-09-13 17:04:58.0  | NULL                    | NULL                   | Other                    | 2011-09-13 17:24:13.0   | CALL BOX: 1 ANGEL ISLAND DR  | NULL             | B99                | 94                    | NULL         | NULL                       | NULL              | 2                       | false             |                          | 1                  | CHIEF              | 8                      | NULL                          | NULL                     | None                   | 112560266-B03  | POINT (-122.421080465833 37.854464340117)  |                     | 2023-11-30 09:35:04.0   | NULL                             | AI            |
| 131460243        | E08              | 13049562             | Watercraft in Distress  | 2013-05-26 00:00:00.0  | 2013-05-26 00:00:00.0  | 2013-05-26 14:23:50.0  | 2013-05-26 14:26:39.0  | 2013-05-26 14:30:08.0  | 2013-05-26 14:31:39.0  | 2013-05-26 14:33:56.0  | NULL                    | NULL                   | Other                    | 2013-05-26 14:37:18.0   | CALL BOX: 1 ANGEL ISLAND DR  | NULL             | B99                | 94                    | NULL         | 3                          | 3                 | 3                       | true              | Fire                     | 1                  | ENGINE             | 1                      | NULL                          | NULL                     | None                   | 131460243-E08  | POINT (-122.421080465833 37.854464340117)  |                     | 2023-11-30 09:35:04.0   | NULL                             | AI            |
+------------------+------------------+----------------------+-------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+-------------------------+------------------------+--------------------------+-------------------------+------------------------------+------------------+--------------------+-----------------------+--------------+----------------------------+-------------------+-------------------------+-------------------+--------------------------+--------------------+--------------------+------------------------+-------------------------------+--------------------------+------------------------+----------------+--------------------------------------------+---------------------+-------------------------+----------------------------------+---------------+



SELECT * FROM re_data WHERE city = 'San Francisco' LIMIT 10;

+------------------+------------------+----------------------+--------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+-------------------------+------------------------+-----------------------------+-------------------------+-------------------------------+------------------+--------------------+-----------------------+--------------+----------------------------+-------------------+-------------------------+-------------------+-------------------------------+--------------------+--------------------+------------------------+-------------------------------+--------------------------+---------------------------------+-----------------+-------------------------------------------------+---------------------+-------------------------+----------------------------------+----------------+
| re_data.call_no  | re_data.unit_id  | re_data.incident_no  | re_data.call_type  |   re_data.call_date    |   re_data.watch_date   | re_data.received_dttm  |   re_data.entry_dttm   | re_data.dispatch_dttm  | re_data.response_dttm  |  re_data.onscene_dttm  | re_data.transport_dttm  | re_data.hospital_dttm  |   re_data.call_final_disp   | re_data.available_dttm  |        re_data.address        | re_data.zipcode  | re_data.battalion  | re_data.station_area  | re_data.box  | re_data.original_priority  | re_data.priority  | re_data.final_priority  | re_data.als_unit  |    re_data.call_type_group    | re_data.no_alarms  | re_data.unit_type  | re_data.unit_sequence  | re_data.fire_prevention_dist  | re_data.supervisor_dist  |      re_data.neighborhoods      |  re_data.rowid  |              re_data.case_location              | re_data.data_as_of  | re_data.data_loaded_at  | re_data.analysis_neighbourhoods  |  re_data.city  |
+------------------+------------------+----------------------+--------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+-------------------------+------------------------+-----------------------------+-------------------------+-------------------------------+------------------+--------------------+-----------------------+--------------+----------------------------+-------------------+-------------------------+-------------------+-------------------------------+--------------------+--------------------+------------------------+-------------------------------+--------------------------+---------------------------------+-----------------+-------------------------------------------------+---------------------+-------------------------+----------------------------------+----------------+
| 191004488        | E36              | 19042782             | Traffic Collision  | 2019-04-10 00:00:00.0  | 2019-04-10 00:00:00.0  | 2019-04-10 22:05:09.0  | 2019-04-10 22:05:09.0  | 2019-04-10 22:05:59.0  | 2019-04-10 22:07:22.0  | 2019-04-10 22:08:57.0  | NULL                    | NULL                   | Code 2 Transport            | 2019-04-10 22:26:28.0   | 13TH ST/MISSION ST            | 94103            | B02                | 36                    | 5124         | NULL                       | 3                 | 3                       | true              | Potentially Life-Threatening  | 1                  | ENGINE             | 1                      | 2                             | 6                        | Mission                         | 191004488-E36   | POINT (-122.420008323243 37.770111448922)       |                     | 2023-11-30 09:35:04.0   | 20                               | San Francisco  |
| 191700420        | 78               | 19072663             | Medical Incident   | 2019-06-19 00:00:00.0  | 2019-06-18 00:00:00.0  | 2019-06-19 05:51:38.0  | 2019-06-19 05:52:59.0  | 2019-06-19 05:53:40.0  | 2019-06-19 05:54:14.0  | NULL                   | NULL                    | NULL                   | Code 2 Transport            | 2019-06-19 05:55:26.0   | 2700 Block of CALIFORNIA ST   | 94115            | B04                | 38                    | 4125         | 2                          | 2                 | 2                       | true              | Non Life-threatening          | 1                  | MEDIC              | 2                      | 4                             | 2                        | Pacific Heights                 | 191700420-78    | POINT (-122.4396176408205 37.78822702457475)    |                     | 2023-11-30 09:35:04.0   | 30                               | San Francisco  |
| 191513208        | 77               | 19064027             | Medical Incident   | 2019-05-31 00:00:00.0  | 2019-05-31 00:00:00.0  | 2019-05-31 19:17:12.0  | 2019-05-31 19:18:29.0  | 2019-05-31 19:19:04.0  | 2019-05-31 19:19:20.0  | NULL                   | NULL                    | NULL                   | Code 2 Transport            | 2019-05-31 19:26:42.0   | CLAY ST/DRUMM ST              | 94111            | B01                | 13                    | 1133         | 2                          | 2                 | 2                       | true              | Non Life-threatening          | 1                  | MEDIC              | 3                      | 1                             | 3                        | Financial District/South Beach  | 191513208-77    | POINT (-122.39677157392984 37.795466113999005)  |                     | 2023-11-30 09:35:04.0   | 8                                | San Francisco  |
| 182852264        | T12              | 18119658             | Structure Fire     | 2018-10-12 00:00:00.0  | 2018-10-12 00:00:00.0  | 2018-10-12 14:23:35.0  | 2018-10-12 14:24:29.0  | 2018-10-12 14:24:51.0  | 2018-10-12 14:25:54.0  | 2018-10-12 14:26:52.0  | NULL                    | NULL                   | Fire                        | 2018-10-12 14:59:52.0   | 100 Block of GRATTAN ST       | 94117            | B05                | 12                    | 5161         | 3                          | 3                 | 3                       | false             | Fire                          | 1                  | TRUCK              | 2                      | 5                             | 5                        | Haight Ashbury                  | 182852264-T12   | POINT (-122.450332225969 37.763988849809)       |                     | 2023-11-30 09:35:04.0   | 3                                | San Francisco  |
| 190263816        | T05              | 19011095             | Alarms             | 2019-01-26 00:00:00.0  | 2019-01-26 00:00:00.0  | 2019-01-26 22:09:31.0  | 2019-01-26 22:11:24.0  | 2019-01-26 22:11:30.0  | 2019-01-26 22:12:23.0  | 2019-01-26 22:15:38.0  | NULL                    | NULL                   | Fire                        | 2019-01-26 22:34:52.0   | 1400 Block of TURK ST         | 94115            | B05                | 05                    | 3535         | 3                          | 3                 | 3                       | false             | Alarm                         | 1                  | TRUCK              | 3                      | 5                             | 5                        | Western Addition                | 190263816-T05   | POINT (-122.432973442897 37.780445711699)       |                     | 2023-11-30 09:35:04.0   | 39                               | San Francisco  |
| 182410228        | 78               | 18101287             | Medical Incident   | 2018-08-29 00:00:00.0  | 2018-08-28 00:00:00.0  | 2018-08-29 02:49:29.0  | 2018-08-29 02:49:29.0  | 2018-08-29 02:50:44.0  | 2018-08-29 02:50:47.0  | 2018-08-29 03:01:31.0  | NULL                    | NULL                   | Unable to Locate            | 2018-08-29 03:04:27.0   | 0 Block of ALLISON ST         | 94112            | B09                | 43                    | 6176         | NULL                       | 2                 | 2                       | true              | Non Life-threatening          | 1                  | MEDIC              | 1                      | 9                             | 11                       | Excelsior                       | 182410228-78    | POINT (-122.44241661365 37.714145484185)        |                     | 2023-11-30 09:35:04.0   | 7                                | San Francisco  |
| 190704136        | 79               | 19030088             | Medical Incident   | 2019-03-11 00:00:00.0  | 2019-03-11 00:00:00.0  | 2019-03-11 22:49:21.0  | 2019-03-11 22:50:02.0  | 2019-03-11 22:50:38.0  | 2019-03-11 22:50:51.0  | 2019-03-11 22:56:27.0  | 2019-03-11 23:19:55.0   | 2019-03-11 23:26:56.0  | Code 2 Transport            | 2019-03-11 22:58:38.0   | 400 Block of ELLIS ST         | 94102            | B03                | 03                    | 1461         | 3                          | 3                 | 3                       | true              | Potentially Life-Threatening  | 1                  | MEDIC              | 1                      | 2                             | 6                        | Tenderloin                      | 190704136-79    | POINT (-122.413593512876 37.784698921976)       |                     | 2023-11-30 09:35:04.0   | 36                               | San Francisco  |
| 191401052        | 86               | 19059301             | Medical Incident   | 2019-05-20 00:00:00.0  | 2019-05-20 00:00:00.0  | 2019-05-20 09:55:30.0  | 2019-05-20 09:56:55.0  | 2019-05-20 09:57:24.0  | 2019-05-20 09:57:30.0  | 2019-05-20 10:04:30.0  | 2019-05-20 10:10:41.0   | 2019-05-20 10:35:25.0  | Code 2 Transport            | 2019-05-20 11:15:57.0   | 300 Block of GOLDEN GATE AVE  | 94102            | B02                | 03                    | 1644         | 2                          | 2                 | 2                       | true              | Non Life-threatening          | 1                  | MEDIC              | 1                      | 2                             | 6                        | Tenderloin                      | 191401052-86    | POINT (-122.41633625179564 37.781605127270446)  |                     | 2023-11-30 09:35:04.0   | 36                               | San Francisco  |
| 190592580        | E08              | 19025122             | Medical Incident   | 2019-02-28 00:00:00.0  | 2019-02-28 00:00:00.0  | 2019-02-28 15:17:43.0  | 2019-02-28 15:18:31.0  | 2019-02-28 15:19:55.0  | 2019-02-28 15:21:13.0  | 2019-02-28 15:25:13.0  | NULL                    | NULL                   | Patient Declined Transport  | 2019-02-28 15:28:09.0   | 400 Block of MINNA ST         | 94103            | B03                | 01                    | 2251         | 3                          | 3                 | 3                       | true              | Potentially Life-Threatening  | 1                  | ENGINE             | 1                      | 3                             | 6                        | South of Market                 | 190592580-E08   | POINT (-122.407387172098 37.781068891878)       |                     | 2023-11-30 09:35:04.0   | 34                               | San Francisco  |
| 190630044        | QRV1             | 19026646             | Medical Incident   | 2019-03-04 00:00:00.0  | 2019-03-03 00:00:00.0  | 2019-03-04 00:18:25.0  | 2019-03-04 00:21:07.0  | 2019-03-04 00:21:20.0  | 2019-03-04 00:21:28.0  | 2019-03-04 00:23:16.0  | NULL                    | NULL                   | Code 2 Transport            | 2019-03-04 00:33:11.0   | 200 Block of LEAVENWORTH ST   | 94102            | B02                | 03                    | 1545         | 3                          | 2                 | 2                       | true              | Potentially Life-Threatening  | 1                  | SUPPORT            | 1                      | 2                             | 6                        | Tenderloin                      | 190630044-QRV1  | POINT (-122.414216002993 37.783250415092)       |                     | 2023-11-30 09:35:04.0   | 36                               | San Francisco  |
+------------------+------------------+----------------------+--------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+------------------------+-------------------------+------------------------+-----------------------------+-------------------------+-------------------------------+------------------+--------------------+-----------------------+--------------+----------------------------+-------------------+-------------------------+-------------------+-------------------------------+--------------------+--------------------+------------------------+-------------------------------+--------------------------+---------------------------------+-----------------+-------------------------------------------------+---------------------+-------------------------+----------------------------------+----------------+





7. Execute following queries on re dataset.


1. How many distinct types of calls were made to the re department?

SELECT COUNT(DISTINCT call_type) call_type_cnt FROM re_data;
+----------------+
| call_type_cnt  |
+----------------+
| 33             |
+----------------+


2. What are distinct types of calls made to the re department?

SELECT DISTINCT call_type FROM re_data;

+-----------------------------------------------+
|                   call_type                   |
+-----------------------------------------------+
| Administrative                                |
| Alarms                                        |
| Electrical Hazard                             |
| Elevator / Escalator Rescue                   |
| Fuel Spill                                    |
| Gas Leak (Natural and LP Gases)               |
| High Angle Rescue                             |
| Marine Fire                                   |
| Mutual Aid / Assist Outside Agency            |
| Odor (Strange / Unknown)                      |
| Oil Spill                                     |
| Other                                         |
| Structure Fire / Smoke in Building            |
| Vehicle Fire                                  |
| Water Rescue                                  |
| Aircraft Emergency                            |
| Assist Police                                 |
| Citizen Assist / Service Call                 |
| Confined Space / Structure Collapse           |
| Explosion                                     |
| Extrication / Entrapped (Machinery, Vehicle)  |
| HazMat                                        |
| Industrial Accidents                          |
| Lightning Strike (Investigation)              |
| Medical Incident                              |
| Outside Fire                                  |
| Smoke Investigation (Outside)                 |
| Structure Fire                                |
| Suspicious Package                            |
| Traffic Collision                             |
| Train / Rail Fire                             |
| Train / Rail Incident                         |
| Watercraft in Distress                        |
+-----------------------------------------------+



3. Find out all responses for delayed times greater than 5 mins?


SELECT DISTINCT call_final_disp FROM re_data 
WHERE (unix_timestamp(response_dttm) - unix_timestamp(received_dttm))/60= 5;

+-----------------------------+
|       call_final_disp       |
+-----------------------------+
| Against Medical Advice      |
| Code 2 Transport            |
| Fire                        |
| Medical Examiner            |
| No Merit                    |
| Other                       |
| Patient Declined Transport  |
| Unable to Locate            |
| CHP                         |
| Cancelled                   |
| Code 3 Transport            |
| Gone on Arrival             |
| SFPD                        |
+-----------------------------+



4. What were the most common call types?

SELECT call_type, COUNT(call_type) cnt FROM re_data
GROUP BY call_type ORDER BY 2 DESC;
+-----------------------------------------------+----------+
|                   call_type                   |   cnt    |
+-----------------------------------------------+----------+
| Medical Incident                              | 4247943  |
| Alarms                                        | 720968   |
| Structure Fire                                | 714873   |
| Traffic Collision                             | 259541   |
| Other                                         | 110855   |
| Citizen Assist / Service Call                 | 96222    |
| Outside Fire                                  | 85967    |
| Water Rescue                                  | 34061    |
| Gas Leak (Natural and LP Gases)               | 30484    |
| Vehicle Fire                                  | 28378    |
| Electrical Hazard                             | 21907    |
| Structure Fire / Smoke in Building            | 18894    |
| Elevator / Escalator Rescue                   | 17952    |
| Smoke Investigation (Outside)                 | 14613    |
| Odor (Strange / Unknown)                      | 13673    |
| Fuel Spill                                    | 7038     |
| HazMat                                        | 4399     |
| Industrial Accidents                          | 3333     |
| Explosion                                     | 3067     |
| Train / Rail Incident                         | 1715     |
| Aircraft Emergency                            | 1512     |
| Assist Police                                 | 1508     |
| High Angle Rescue                             | 1456     |
| Watercraft in Distress                        | 1237     |
| Extrication / Entrapped (Machinery, Vehicle)  | 935      |
| Confined Space / Structure Collapse           | 791      |
| Mutual Aid / Assist Outside Agency            | 626      |
| Oil Spill                                     | 518      |
| Marine Fire                                   | 508      |
| Suspicious Package                            | 368      |
| Administrative                                | 345      |
| Train / Rail Fire                             | 120      |
| Lightning Strike (Investigation)              | 21       |
+-----------------------------------------------+----------+


SELECT call_type FROM re_data
GROUP BY call_type ORDER BY COUNT(call_type) DESC LIMIT 5;

+--------------------+
|     call_type      |
+--------------------+
| Medical Incident   |
| Alarms             |
| Structure Fire     |
| Traffic Collision  |
| Other              |
+--------------------+


5. What zip codes accounted for the most common calls?

SELECT zipcode, call_type, COUNT(call_type) cnt FROM re_data
GROUP BY zipcode, call_type ORDER BY 3 DESC LIMIT 10;
+----------+-------------------+---------+
| zipcode  |     call_type     |   cnt   |
+----------+-------------------+---------+
| 94102    | Medical Incident  | 616021  |
| 94103    | Medical Incident  | 577576  |
| 94109    | Medical Incident  | 372844  |
| 94110    | Medical Incident  | 363740  |
| 94124    | Medical Incident  | 218131  |
| 94112    | Medical Incident  | 205392  |
| 94115    | Medical Incident  | 175661  |
| 94107    | Medical Incident  | 154910  |
| 94122    | Medical Incident  | 152124  |
| 94133    | Medical Incident  | 141632  |
+----------+-------------------+---------+


6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

SELECT DISTINCT neighborhoods FROM re_data WHERE city = 'San Francisco' AND zipcode IN (94102,94103);

+---------------------------------+
|          neighborhoods          |
+---------------------------------+
| Castro/Upper Market             |
| Financial District/South Beach  |
| Hayes Valley                    |
| Mission                         |
| Mission Bay                     |
| Nob Hill                        |
| Potrero Hill                    |
| South of Market                 |
| Tenderloin                      |
| Western Addition                |
+---------------------------------+


7. What was the sum of all calls, average, min, and max of the call response times?

SELECT SUM(unix_timestamp(response_dttm) - unix_timestamp(received_dttm))/60 sum,
AVG(unix_timestamp(response_dttm) - unix_timestamp(received_dttm))/60 avg,
MIN(unix_timestamp(response_dttm) - unix_timestamp(received_dttm))/60 min,
MAX(unix_timestamp(response_dttm) - unix_timestamp(received_dttm))/60 max 
FROM re_data;

+-----------------------+--------------------+---------------------+---------------------+
|          sum          |        avg         |         min         |         max         |
+-----------------------+--------------------+---------------------+---------------------+
| 2.4017975983333334E7  | 4.001277448558066  | -713.4666666666667  | 2465.2833333333333  |
+-----------------------+--------------------+---------------------+---------------------+


8. How many distinct years of data are in the CSV le?

SELECT COUNT(DISTINCT YEAR(call_date)) year_cnt FROM re_data;
+-----------+
| year_cnt  |
+-----------+
| 24        |
+-----------+

9. What week of the year in 2018 had the most re calls?

SELECT WEEKOFYEAR(call_date) week, COUNT(call_no) cnt FROM re_data 
WHERE YEAR(call_date) = 2018 GROUP BY WEEKOFYEAR(call_date) ORDER BY 2 DESC LIMIT 1;

+-------+-------+
| week  |  cnt  |
+-------+-------+
| 1     | 7545  |
+-------+-------+


SELECT WEEKOFYEAR(call_date) week, COUNT(call_no) cnt FROM re_data 
WHERE YEAR(call_date) = 2018 GROUP BY WEEKOFYEAR(call_date) ORDER BY 2 DESC LIMIT 5;

+-------+-------+
| week  |  cnt  |
+-------+-------+
| 1     | 7545  |
| 25    | 6425  |
| 49    | 6354  |
| 22    | 6328  |
| 13    | 6321  |
+-------+-------+



10. What neighborhoods in San Francisco had the worst response time in 2018?

SELECT DISTINCT neighborhoods, (unix_timestamp(response_dttm) - unix_timestamp(received_dttm))/60 AS response_time_min FROM re_data 
WHERE city = 'San Francisco' AND YEAR(call_date)=2018
ORDER BY 2 DESC
LIMIT 3;

+------------------------+--------------------+
|     neighborhoods      | response_time_min  |
+------------------------+--------------------+
| West of Twin Peaks     | 754.0833333333334  |
| Chinatown              | 734.8666666666667  |
| Bayview Hunters Point  | 715.7666666666667  |
+------------------------+--------------------+
