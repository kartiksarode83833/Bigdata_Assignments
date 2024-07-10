
____________________________________________________________________________________________________________________________________________________________

2. Execute following queries on "emp" and "dept" dataset.

1. Create table "emp_staging" and load data from emp.csv in it.
 
CREATE TABLE emp_staging(
empno INT,
ename STRING,
job STRING,
mgr INT,
hire DATE,
sal DOUBLE,
comm DOUBLE,
deptno INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL
INPATH "/home/sunbeam/CDAC/BigData/data/emp.csv"
INTO TABLE emp_staging;

SELECT * FROM  emp_staging;
+--------------------+--------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+
| emp_staging.empno  | emp_staging.ename  | emp_staging.job  | emp_staging.mgr  | emp_staging.hire  | emp_staging.sal  | emp_staging.comm  | emp_staging.deptno  |
+--------------------+--------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+
| 7369               | SMITH              | CLERK            | 7902             | 1980-12-17        | 800.0            | NULL              | 20                  |
| 7499               | ALLEN              | SALESMAN         | 7698             | 1981-02-20        | 1600.0           | 300.0             | 30                  |
| 7521               | WARD               | SALESMAN         | 7698             | 1981-02-22        | 1250.0           | 500.0             | 30                  |
| 7566               | JONES              | MANAGER          | 7839             | 1981-04-02        | 2975.0           | NULL              | 20                  |
| 7654               | MARTIN             | SALESMAN         | 7698             | 1981-09-28        | 1250.0           | 1400.0            | 30                  |
| 7698               | BLAKE              | MANAGER          | 7839             | 1981-05-01        | 2850.0           | NULL              | 30                  |
| 7782               | CLARK              | MANAGER          | 7839             | 1981-06-09        | 2450.0           | NULL              | 10                  |
| 7788               | SCOTT              | ANALYST          | 7566             | 1982-12-09        | 3000.0           | NULL              | 20                  |
| 7839               | KING               | PRESIDENT        | NULL             | 1981-11-17        | 5000.0           | NULL              | 10                  |
| 7844               | TURNER             | SALESMAN         | 7698             | 1981-09-08        | 1500.0           | 0.0               | 30                  |
| 7876               | ADAMS              | CLERK            | 7788             | 1983-01-12        | 1100.0           | NULL              | 20                  |
| 7900               | JAMES              | CLERK            | 7698             | 1981-12-03        | 950.0            | NULL              | 30                  |
| 7902               | FORD               | ANALYST          | 7566             | 1981-12-03        | 3000.0           | NULL              | 20                  |
| 7934               | MILLER             | CLERK            | 7782             | 1982-01-23        | 1300.0           | NULL              | 10                  |
+--------------------+--------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+
14 rows selected (0.157 seconds)


2. Create table "dept_staging" and load data from dept.csv in it.

CREATE TABLE dept_staging(
deptno INT,
dname VARCHAR(40),
loc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL
INPATH '/home/sunbeam/CDAC/BigData/data/dept.csv'
INTO TABLE dept_staging;

SELECT * FROM dept_staging;
+----------------------+---------------------+-------------------+
| dept_staging.deptno  | dept_staging.dname  | dept_staging.loc  |
+----------------------+---------------------+-------------------+
| 10                   | ACCOUNTING          | NEW YORK          |
| 20                   | RESEARCH            | DALLAS            |
| 30                   | SALES               | CHICAGO           |
| 40                   | OPERATIONS          | BOSTON            |
+----------------------+---------------------+-------------------+
4 rows selected (0.211 seconds)


3. Display dept name and number of emps in each dept.

SELECT d.dname dept_name, COUNT(e.empno) Number_of_emps FROM emp_staging e
INNER JOIN dept_staging d ON d.deptno = e.deptno
GROUP BY d.dname;
+-------------+-----------------+
|  dept_name  | number_of_emps  |
+-------------+-----------------+
| ACCOUNTING  | 3               |
| RESEARCH    | 5               |
| SALES       | 6               |
+-------------+-----------------+
3 rows selected (19.676 seconds)

4. Display emp name and his dept name.

select e.ename Emp_Name, d.dname Dept_Name from emp_staging e
inner join dept_staging d on e.deptno = d.deptno

+-----------+-------------+
| emp_name  |  dept_name  |
+-----------+-------------+
| SMITH     | RESEARCH    |
| ALLEN     | SALES       |
| WARD      | SALES       |
| JONES     | RESEARCH    |
| MARTIN    | SALES       |
| BLAKE     | SALES       |
| CLARK     | ACCOUNTING  |
| SCOTT     | RESEARCH    |
| KING      | ACCOUNTING  |
| TURNER    | SALES       |
| ADAMS     | RESEARCH    |
| JAMES     | SALES       |
| FORD      | RESEARCH    |
| MILLER    | ACCOUNTING  |
+-----------+-------------+
14 rows selected (16.362 seconds)

5. Display all emps (name, job, deptno) with their manager (name, job, deptno), who are not in their department.

select e.ename, e.job, e.deptno , m.ename, m.job, m.deptno from emp_staging e inner join emp_staging m on e.mgr = m.empno
where e.deptno <> m.deptno;

+----------+----------+-----------+----------+------------+-----------+
| e.ename  |  e.job   | e.deptno  | m.ename  |   m.job    | m.deptno  |
+----------+----------+-----------+----------+------------+-----------+
| JONES    | MANAGER  | 20        | KING     | PRESIDENT  | 10        |
| BLAKE    | MANAGER  | 30        | KING     | PRESIDENT  | 10        |
+----------+----------+-----------+----------+------------+-----------+
2 rows selected (15.498 seconds)



6. Display all manager names with list of all dept names (where they can work).

select e.ename, d.dname from emp_staging e
inner join dept_staging d on e.deptno = d.deptno
where e.job = "MANAGER" ;

+----------+-------------+
| e.ename  |   d.dname   |
+----------+-------------+
| JONES    | RESEARCH    |
| BLAKE    | SALES       |
| CLARK    | ACCOUNTING  |
+----------+-------------+
3 rows selected (14.876 seconds)



7. Display job-wise total salary along with total salary of all employees.

select job, sum(sal) over(partition by job) Jobwise_Salary, sum(sal) over() Total_Salary from emp_staging;
+------------+-----------------+---------------+
|    job     | jobwise_salary  | total_salary  |
+------------+-----------------+---------------+
| SALESMAN   | 5600.0          | 29025.0       |
| SALESMAN   | 5600.0          | 29025.0       |
| SALESMAN   | 5600.0          | 29025.0       |
| SALESMAN   | 5600.0          | 29025.0       |
| PRESIDENT  | 5000.0          | 29025.0       |
| MANAGER    | 8275.0          | 29025.0       |
| MANAGER    | 8275.0          | 29025.0       |
| MANAGER    | 8275.0          | 29025.0       |
| CLERK      | 4150.0          | 29025.0       |
| CLERK      | 4150.0          | 29025.0       |
| CLERK      | 4150.0          | 29025.0       |
| CLERK      | 4150.0          | 29025.0       |
| ANALYST    | 6000.0          | 29025.0       |
| ANALYST    | 6000.0          | 29025.0       |
+------------+-----------------+---------------+
14 rows selected (32.973 seconds)


8. Display dept-wise total salary along with total salary of all employees.

select deptno, sum(sal) over(partition by deptno) Deptwise_Total_Salary, sum(sal) over() Total_Salary from emp_staging;
+---------+------------------------+---------------+
| deptno  | deptwise_total_salary  | total_salary  |
+---------+------------------------+---------------+
| 30      | 9400.0                 | 29025.0       |
| 30      | 9400.0                 | 29025.0       |
| 30      | 9400.0                 | 29025.0       |
| 30      | 9400.0                 | 29025.0       |
| 30      | 9400.0                 | 29025.0       |
| 30      | 9400.0                 | 29025.0       |
| 20      | 10875.0                | 29025.0       |
| 20      | 10875.0                | 29025.0       |
| 20      | 10875.0                | 29025.0       |
| 20      | 10875.0                | 29025.0       |
| 20      | 10875.0                | 29025.0       |
| 10      | 8750.0                 | 29025.0       |
| 10      | 8750.0                 | 29025.0       |
| 10      | 8750.0                 | 29025.0       |
+---------+------------------------+---------------+
14 rows selected (33.874 seconds)


9. Display per dept job-wise total salary along with total salary of all employees.

select deptno, job, sum(sal) over(partition by deptno, job) Per_Dept_Jobwise_Total_Salary from emp_staging;
+---------+------------+--------------------------------+
| deptno  |    job     | per_dept_jobwise_total_salary  |
+---------+------------+--------------------------------+
| 10      | CLERK      | 1300.0                         |
| 10      | MANAGER    | 2450.0                         |
| 10      | PRESIDENT  | 5000.0                         |
| 20      | ANALYST    | 6000.0                         |
| 20      | ANALYST    | 6000.0                         |
| 20      | CLERK      | 1900.0                         |
| 20      | CLERK      | 1900.0                         |
| 20      | MANAGER    | 2975.0                         |
| 30      | CLERK      | 950.0                          |
| 30      | MANAGER    | 2850.0                         |
| 30      | SALESMAN   | 5600.0                         |
| 30      | SALESMAN   | 5600.0                         |
| 30      | SALESMAN   | 5600.0                         |
| 30      | SALESMAN   | 5600.0                         |
+---------+------------+--------------------------------+
14 rows selected (15.878 seconds)


10. Display number of employees recruited per year in descending order of employee count.

select year(hire) Year, count(empno) Number_Of_Employees_Recruited_Per_Year from emp_staging GROUP BY year(hire) order by 2 desc;

+-------+-----------------------------------------+
| year  | number_of_employees_recruited_per_year  |
+-------+-----------------------------------------+
| 1981  | 10                                      |
| 1982  | 2                                       |
| 1983  | 1                                       |
| 1980  | 1                                       |
+-------+-----------------------------------------+
4 rows selected (35.428 seconds)

11. Display unique job roles who gets commission.

select job, comm from emp_staging where comm is not null and comm<>0;
+-----------+---------+
|    job    |  comm   |
+-----------+---------+
| SALESMAN  | 300.0   |
| SALESMAN  | 500.0   |
| SALESMAN  | 1400.0  |
+-----------+---------+
3 rows selected (0.125 seconds)

select distinct job from emp_staging where comm is not null and comm<>0;
+-----------+
|    job    |
+-----------+
| SALESMAN  |
+-----------+
1 row selected (16.748 seconds)



12. Display dept name in which there is no employee (using sub-query).

select d.dname from dept_staging d
where d.deptno NOT IN (select deptno from emp_staging);

+-------------+
|   d.dname   |
+-------------+
| OPERATIONS  |
+-------------+
1 row selected (63.723 seconds)

13. Display emp-name, dept-name, salary, total salary of that dept (using sub-query).

select e.ename, d.dname, e.sal,sum(e.sal) over(partition by d.deptno) total_salary_of_that_dept from emp_staging e, dept_staging d
where e.deptno = d.deptno;
+----------+-------------+---------+----------------------------+
| e.ename  |   d.dname   |  e.sal  | total_salary_of_that_dept  |
+----------+-------------+---------+----------------------------+
| MILLER   | ACCOUNTING  | 1300.0  | 8750.0                     |
| KING     | ACCOUNTING  | 5000.0  | 8750.0                     |
| CLARK    | ACCOUNTING  | 2450.0  | 8750.0                     |
| ADAMS    | RESEARCH    | 1100.0  | 10875.0                    |
| SCOTT    | RESEARCH    | 3000.0  | 10875.0                    |
| SMITH    | RESEARCH    | 800.0   | 10875.0                    |
| JONES    | RESEARCH    | 2975.0  | 10875.0                    |
| FORD     | RESEARCH    | 3000.0  | 10875.0                    |
| TURNER   | SALES       | 1500.0  | 9400.0                     |
| ALLEN    | SALES       | 1600.0  | 9400.0                     |
| BLAKE    | SALES       | 2850.0  | 9400.0                     |
| MARTIN   | SALES       | 1250.0  | 9400.0                     |
| WARD     | SALES       | 1250.0  | 9400.0                     |
| JAMES    | SALES       | 950.0   | 9400.0                     |
+----------+-------------+---------+----------------------------+
14 rows selected (20.453 seconds)

select e.ename, (select d.dname from dept_staging d where d.deptno= e.deptno) dname, e.sal,
(select  sum(e1.sal) from emp_staging e1 where e1.deptno = e.deptno GROUP by e1.deptno) total_salary_of_that_dept 
from emp_staging e order by 2;
+----------+-------------+---------+----------------------------+
| e.ename  |    dname    |  e.sal  | total_salary_of_that_dept  |
+----------+-------------+---------+----------------------------+
| MILLER   | ACCOUNTING  | 1300.0  | 8750.0                     |
| KING     | ACCOUNTING  | 5000.0  | 8750.0                     |
| CLARK    | ACCOUNTING  | 2450.0  | 8750.0                     |
| ADAMS    | RESEARCH    | 1100.0  | 10875.0                    |
| SCOTT    | RESEARCH    | 3000.0  | 10875.0                    |
| SMITH    | RESEARCH    | 800.0   | 10875.0                    |
| JONES    | RESEARCH    | 2975.0  | 10875.0                    |
| FORD     | RESEARCH    | 3000.0  | 10875.0                    |
| TURNER   | SALES       | 1500.0  | 9400.0                     |
| ALLEN    | SALES       | 1600.0  | 9400.0                     |
| BLAKE    | SALES       | 2850.0  | 9400.0                     |
| MARTIN   | SALES       | 1250.0  | 9400.0                     |
| WARD     | SALES       | 1250.0  | 9400.0                     |
| JAMES    | SALES       | 950.0   | 9400.0                     |
+----------+-------------+---------+----------------------------+
14 rows selected (97.522 seconds)

14. Display all managers and presidents along with number of (immediate) subbordinates.

select *, (select count(empno) from emp_staging e1 where e.empno = e1.mgr) number_of_immediate_subbordinates from emp_staging e where e.job in ('MANAGER','PRESIDENT') ;
 
+----------+----------+------------+--------+-------------+---------+---------+-----------+------------------------------------+
| e.empno  | e.ename  |   e.job    | e.mgr  |   e.hire    |  e.sal  | e.comm  | e.deptno  | number_of_immediate_subbordinates  |
+----------+----------+------------+--------+-------------+---------+---------+-----------+------------------------------------+
| 7566     | JONES    | MANAGER    | 7839   | 1981-04-02  | 2975.0  | NULL    | 20        | 2                                  |
| 7698     | BLAKE    | MANAGER    | 7839   | 1981-05-01  | 2850.0  | NULL    | 30        | 5                                  |
| 7782     | CLARK    | MANAGER    | 7839   | 1981-06-09  | 2450.0  | NULL    | 10        | 1                                  |
| 7839     | KING     | PRESIDENT  | NULL   | 1981-11-17  | 5000.0  | NULL    | 10        | 3                                  |
+----------+----------+------------+--------+-------------+---------+---------+-----------+------------------------------------+
4 rows selected (31.889 seconds)

____________________________________________________________________________________________________________________________________________________________

3. Execute following queries on "emp" and "dept" dataset using CTE.

1. Find emp with max sal of each dept.

with max_sal as (select max(sal) max from emp_staging)
select * from emp_staging where sal = (select max from max_sal);

+--------------------+--------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+
| emp_staging.empno  | emp_staging.ename  | emp_staging.job  | emp_staging.mgr  | emp_staging.hire  | emp_staging.sal  | emp_staging.comm  | emp_staging.deptno  |
+--------------------+--------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+
| 7839               | KING               | PRESIDENT        | NULL             | 1981-11-17        | 5000.0           | NULL              | 10                  |
+--------------------+--------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+
1 row selected (69.416 seconds)

2. Find avg of deptwise total sal.

WITH avg_dept_sal AS (select deptno, avg(sal) avg_sal from emp_staging group by deptno )
select d.dname, (select avg_sal from avg_dept_sal ads where d.deptno = ads.deptno) Deptwise_Average_Sal from dept_staging d ;


3. Compare (show side-by-side) sal of each emp with avg sal in his dept and avg sal for his job.

WITH avg_sal_dept AS (select deptno, avg(sal) avg_sal from emp_staging group by deptno),
avg_sal_job AS (select job, avg(sal) avg_sal from emp_staging group by job)
select e.ename Emp_Name, e.sal Emp_Salary, (select avg_sal from avg_sal_dept asd where e.deptno = asd.deptno) Deptwise_Average_Sal , 
(select avg_sal from avg_sal_job asj where asj.job = e.job) Jobwise_Average_salary from emp_staging e; 

+-----------+-------------+-----------------------+-------------------------+
| emp_name  | emp_salary  | deptwise_average_sal  | jobwise_average_salary  |
+-----------+-------------+-----------------------+-------------------------+
| SMITH     | 800.0       | 2175.0                | 1037.5                  |
| ALLEN     | 1600.0      | 1566.6666666666667    | 1400.0                  |
| WARD      | 1250.0      | 1566.6666666666667    | 1400.0                  |
| JONES     | 2975.0      | 2175.0                | 2758.3333333333335      |
| MARTIN    | 1250.0      | 1566.6666666666667    | 1400.0                  |
| BLAKE     | 2850.0      | 1566.6666666666667    | 2758.3333333333335      |
| CLARK     | 2450.0      | 2916.6666666666665    | 2758.3333333333335      |
| SCOTT     | 3000.0      | 2175.0                | 3000.0                  |
| KING      | 5000.0      | 2916.6666666666665    | 5000.0                  |
| TURNER    | 1500.0      | 1566.6666666666667    | 1400.0                  |
| ADAMS     | 1100.0      | 2175.0                | 1037.5                  |
| JAMES     | 950.0       | 1566.6666666666667    | 1037.5                  |
| FORD      | 3000.0      | 2175.0                | 3000.0                  |
| MILLER    | 1300.0      | 2916.6666666666665    | 1037.5                  |
+-----------+-------------+-----------------------+-------------------------+
14 rows selected (107.86 seconds)


WITH avg_sal_dept AS (select deptno, avg(sal) avg_sal from emp_staging  group by deptno),
avg_sal_job AS (select job, avg(sal) avg_sal from emp_staging  group by job)
select
    e.ename ,
    e.sal, 
    asd.avg_sal Deptwise_Average_Sal,
    asj.avg_sal Jobwise_Average_salary
from  emp_staging e 
inner join avg_sal_dept asd on e.deptno = asd.deptno
inner join avg_sal_job asj on e.job = asj.job;


4. Divide emps by category -- Poor < 1500, 1500 <= Middle <= 2500, Rich > 2500. Hint: CASE ... WHEN. Count emps for each category.

WITH emp_cat AS (
    select CASE 
        WHEN sal<1500 THEN 'Poor'
        WHEN 1500<= sal and sal <= 2500 THEN 'Middle'
        WHEN sal > 2500 THEN 'Rich'
        END
        category,
        empno
    from emp_staging   
)
select category , count(empno) count_of_emp_catwise from emp_cat group by category ;

+-----------+-----------------------+
| category  | count_of_emp_catwise  |
+-----------+-----------------------+
| Middle    | 3                     |
| Poor      | 6                     |
| Rich      | 5                     |
+-----------+-----------------------+
3 rows selected (17.742 seconds)

5. Display emps with category (as above), empno, ename, sal and dname.

WITH emp_cat AS(
    select CASE
        WHEN sal<1500 THEN 'Poor'
        WHEN 1500<= sal and sal <= 2500 THEN 'Middle'
        WHEN 2500 < sal THEN 'Rich' 
        END category,
        empno,
        ename,
        sal salary,
        deptno
    from emp_staging    
)
select category, empno, ename, salary, (select d.dname dept_name from dept_staging d where d.deptno = emp_cat.deptno) dept_name
from emp_cat;

+-----------+--------+---------+---------+-------------+
| category  | empno  |  ename  | salary  |  dept_name  |
+-----------+--------+---------+---------+-------------+
| Poor      | 7369   | SMITH   | 800.0   | RESEARCH    |
| Middle    | 7499   | ALLEN   | 1600.0  | SALES       |
| Poor      | 7521   | WARD    | 1250.0  | SALES       |
| Rich      | 7566   | JONES   | 2975.0  | RESEARCH    |
| Poor      | 7654   | MARTIN  | 1250.0  | SALES       |
| Rich      | 7698   | BLAKE   | 2850.0  | SALES       |
| Middle    | 7782   | CLARK   | 2450.0  | ACCOUNTING  |
| Rich      | 7788   | SCOTT   | 3000.0  | RESEARCH    |
| Rich      | 7839   | KING    | 5000.0  | ACCOUNTING  |
| Middle    | 7844   | TURNER  | 1500.0  | SALES       |
| Poor      | 7876   | ADAMS   | 1100.0  | RESEARCH    |
| Poor      | 7900   | JAMES   | 950.0   | SALES       |
| Rich      | 7902   | FORD    | 3000.0  | RESEARCH    |
| Poor      | 7934   | MILLER  | 1300.0  | ACCOUNTING  |
+-----------+--------+---------+---------+-------------+
14 rows selected (32.604 seconds)


6. Count number of emps in each dept for each category (as above).

WITH emp_cat AS(
    select CASE
        WHEN sal<1500 THEN 'Poor'
        WHEN 1500<= sal and sal <= 2500 THEN 'Middle'
        WHEN 2500 < sal THEN 'Rich' 
        END category,
        empno,
        deptno
    from emp_staging    
)
select (select d.dname from dept_staging d where d.deptno = ec.deptno) dept_name, ec.category, count(ec.empno) count_of_emp_deptwise_catwise
from emp_cat ec
group by ec.deptno, ec.category;

+-------------+--------------+------+
|  dept_name  | ec.category  | _c2  |
+-------------+--------------+------+
| ACCOUNTING  | Middle       | 1    |
| ACCOUNTING  | Poor         | 1    |
| ACCOUNTING  | Rich         | 1    |
| RESEARCH    | Poor         | 2    |
| RESEARCH    | Rich         | 3    |
| SALES       | Middle       | 2    |
| SALES       | Poor         | 3    |
| SALES       | Rich         | 1    |
+-------------+--------------+------+
8 rows selected (50.692 seconds)

____________________________________________________________________________________________________________________________________________________________

4. Execute following queries for books.csv dataset.

1. Create table "books_staging" and load books.csv in it.

CREATE TABLE books_staging(
    id INT,
    name STRING,
    author STRING,
    subject STRING,
    price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL
INPATH '/home/sunbeam/CDAC/BigData/data/books.csv'
INTO TABLE books_staging;

select * from books_staging;

+-----------+----------------------------------+--------------------+--------------------+--------------+
| books.id  |            books.name            |    books.author    |   books.subject    | books.price  |
+-----------+----------------------------------+--------------------+--------------------+--------------+
| 1001      | Exploring C                      | Yashwant Kanetkar  | C Programming      | 123.456      |
| 1002      | Pointers in C                    | Yashwant Kanetkar  | C Programming      | 371.019      |
| 1003      | ANSI C Programming               | E Balaguruswami    | C Programming      | 334.215      |
| 1004      | ANSI C Programming               | Dennis Ritchie     | C Programming      | 140.121      |
| 2001      | C++ Complete Reference           | Herbert Schildt    | C++ Programming    | 417.764      |
| 2002      | C++ Primer                       | Stanley Lippman    | C++ Programming    | 620.665      |
| 2003      | C++ Programming Language         | Bjarne Stroustrup  | C++ Programming    | 987.213      |
| 3001      | Java Complete Reference          | Herbert Schildt    | Java Programming   | 525.121      |
| 3002      | Core Java Volume I               | Cay Horstmann      | Java Programming   | 575.651      |
| 3003      | Java Programming Language        | James Gosling      | Java Programming   | 458.238      |
| 4001      | Operatig System Concepts         | Peter Galvin       | Operating Systems  | 567.391      |
| 4002      | Design of UNIX Operating System  | Mauris J Bach      | Operating Systems  | 421.938      |
| 4003      | UNIX Internals                   | Uresh Vahalia      | Operating Systems  | 352.822      |
+-----------+----------------------------------+--------------------+--------------------+--------------+
13 rows selected (0.147 seconds)

2. Create table "books_orc" as transactional table.

CREATE TABLE books_orc(
    id INT,
    name STRING,
    author STRING,
    subject STRING,
    price DOUBLE
)
STORED AS ORC
TBLPROPERTIES('transactional'='true');

insert into books_orc
select * from books_staging;

select * from books_orc;
+-----------+----------------------------------+--------------------+--------------------+--------------+
| books.id  |            books.name            |    books.author    |   books.subject    | books.price  |
+-----------+----------------------------------+--------------------+--------------------+--------------+
| 1001      | Exploring C                      | Yashwant Kanetkar  | C Programming      | 123.456      |
| 1002      | Pointers in C                    | Yashwant Kanetkar  | C Programming      | 371.019      |
| 1003      | ANSI C Programming               | E Balaguruswami    | C Programming      | 334.215      |
| 1004      | ANSI C Programming               | Dennis Ritchie     | C Programming      | 140.121      |
| 2001      | C++ Complete Reference           | Herbert Schildt    | C++ Programming    | 417.764      |
| 2002      | C++ Primer                       | Stanley Lippman    | C++ Programming    | 620.665      |
| 2003      | C++ Programming Language         | Bjarne Stroustrup  | C++ Programming    | 987.213      |
| 3001      | Java Complete Reference          | Herbert Schildt    | Java Programming   | 525.121      |
| 3002      | Core Java Volume I               | Cay Horstmann      | Java Programming   | 575.651      |
| 3003      | Java Programming Language        | James Gosling      | Java Programming   | 458.238      |
| 4001      | Operatig System Concepts         | Peter Galvin       | Operating Systems  | 567.391      |
| 4002      | Design of UNIX Operating System  | Mauris J Bach      | Operating Systems  | 421.938      |
| 4003      | UNIX Internals                   | Uresh Vahalia      | Operating Systems  | 352.822      |
+-----------+----------------------------------+--------------------+--------------------+--------------+
13 rows selected (0.198 seconds)

3. Create a materialized view for summary -- Subjectwise average book price.

CREATE materialized view summary AS (select subject, ROUND(avg(price),2) average_price  from books_orc group by subject);

select * from summary;
+--------------------+------------------------+
|  summary.subject   | summary.average_price  |
+--------------------+------------------------+
| C Programming      | 242.2                  |
| C++ Programming    | 675.21                 |
| Java Programming   | 519.67                 |
| Operating Systems  | 447.38                 |
+--------------------+------------------------+
4 rows selected (0.217 seconds)

4. Display a report that shows subject and average price in descending order -- on materialized view.

CREATE MATERIALIZED VIEW subjectwise_avg_price AS (select subject,ROUND(avg(price),2) average_price from books_orc group by subject order by 2 DESC);

SELECT * FROM subjectwise_avg_price;
+--------------------------------+--------------------------------------+
| subjectwise_avg_price.subject  | subjectwise_avg_price.average_price  |
+--------------------------------+--------------------------------------+
| C++ Programming                | 675.21                               |
| Java Programming               | 519.67                               |
| Operating Systems              | 447.38                               |
| C Programming                  | 242.2                                |
+--------------------------------+--------------------------------------+
4 rows selected (0.135 seconds)


5. Create a new le newbooks.csv.

        20,Atlas Shrugged,Ayn Rand,Novel,723.90
        21,The Fountainhead,Ayn Rand,Novel,923.80
        22,The Archer,Paulo Cohelo,Novel,623.94
        23,The Alchemist,Paulo Cohelo,Novel,634.80

touch newbooks.csv

6. Upload the le newbooks.csv into books_staging.

LOAD DATA LOCAL
INPATH '/home/sunbeam/Desktop/Assignment/Big_Data/newbooks.csv.'
into table books_staging;

select * from books_staging;

+-------------------+----------------------------------+-----------------------+------------------------+----------------------+
| books_staging.id  |        books_staging.name        | books_staging.author  | books_staging.subject  | books_staging.price  |
+-------------------+----------------------------------+-----------------------+------------------------+----------------------+
| 1001              | Exploring C                      | Yashwant Kanetkar     | C Programming          | 123.456              |
| 1002              | Pointers in C                    | Yashwant Kanetkar     | C Programming          | 371.019              |
| 1003              | ANSI C Programming               | E Balaguruswami       | C Programming          | 334.215              |
| 1004              | ANSI C Programming               | Dennis Ritchie        | C Programming          | 140.121              |
| 2001              | C++ Complete Reference           | Herbert Schildt       | C++ Programming        | 417.764              |
| 2002              | C++ Primer                       | Stanley Lippman       | C++ Programming        | 620.665              |
| 2003              | C++ Programming Language         | Bjarne Stroustrup     | C++ Programming        | 987.213              |
| 3001              | Java Complete Reference          | Herbert Schildt       | Java Programming       | 525.121              |
| 3002              | Core Java Volume I               | Cay Horstmann         | Java Programming       | 575.651              |
| 3003              | Java Programming Language        | James Gosling         | Java Programming       | 458.238              |
| 4001              | Operatig System Concepts         | Peter Galvin          | Operating Systems      | 567.391              |
| 4002              | Design of UNIX Operating System  | Mauris J Bach         | Operating Systems      | 421.938              |
| 4003              | UNIX Internals                   | Uresh Vahalia         | Operating Systems      | 352.822              |
| 20                | Atlas Shrugged                   | Ayn Rand              | Novel                  | 723.9                |
| 21                | The Fountainhead                 | Ayn Rand              | Novel                  | 923.8                |
| 22                | The Archer                       | Paulo Cohelo          | Novel                  | 623.94               |
| 23                | The Alchemist                    | Paulo Cohelo          | Novel                  | 634.8                |
+-------------------+----------------------------------+-----------------------+------------------------+----------------------+
17 rows selected (0.139 seconds)

==================================================  OR =========================================================

6. Upload the le newbooks.csv into books_staging.

CREATE TABLE books_staging_2(
    bookid INT,
    book_name STRING,
    author STRING,
    subject STRING,
    price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

show tables;
+------------------------+
|        tab_name        |
+------------------------+
| books                  |
| books_staging          |
| books_staging_2        |
| dept_staging           |
| emp_staging            |
| subjectwise_avg_price  |
| summary                |
+------------------------+
7 rows selected (0.053 seconds)


LOAD DATA LOCAL
INPATH '/home/sunbeam/Desktop/Assignment/Big_Data/newbooks.csv.'
INTO TABLE books_staging_2;

select * from books_staging_2;
+-------------------------+----------------------------+-------------------------+--------------------------+------------------------+
| books_staging_2.bookid  | books_staging_2.book_name  | books_staging_2.author  | books_staging_2.subject  | books_staging_2.price  |
+-------------------------+----------------------------+-------------------------+--------------------------+------------------------+
| 20                      | Atlas Shrugged             | Ayn Rand                | Novel                    | 723.9                  |
| 21                      | The Fountainhead           | Ayn Rand                | Novel                    | 923.8                  |
| 22                      | The Archer                 | Paulo Cohelo            | Novel                    | 623.94                 |
| 23                      | The Alchemist              | Paulo Cohelo            | Novel                    | 634.8                  |
+-------------------------+----------------------------+-------------------------+--------------------------+------------------------+
4 rows selected (0.211 seconds)        

insert into books_staging  
select * from books_staging_2;
+-------------------+----------------------------------+-----------------------+------------------------+----------------------+
| books_staging.id  |        books_staging.name        | books_staging.author  | books_staging.subject  | books_staging.price  |
+-------------------+----------------------------------+-----------------------+------------------------+----------------------+
| 20                | Atlas Shrugged                   | Ayn Rand              | Novel                  | 723.9                |
| 21                | The Fountainhead                 | Ayn Rand              | Novel                  | 923.8                |
| 22                | The Archer                       | Paulo Cohelo          | Novel                  | 623.94               |
| 23                | The Alchemist                    | Paulo Cohelo          | Novel                  | 634.8                |
| 1001              | Exploring C                      | Yashwant Kanetkar     | C Programming          | 123.456              |
| 1002              | Pointers in C                    | Yashwant Kanetkar     | C Programming          | 371.019              |
| 1003              | ANSI C Programming               | E Balaguruswami       | C Programming          | 334.215              |
| 1004              | ANSI C Programming               | Dennis Ritchie        | C Programming          | 140.121              |
| 2001              | C++ Complete Reference           | Herbert Schildt       | C++ Programming        | 417.764              |
| 2002              | C++ Primer                       | Stanley Lippman       | C++ Programming        | 620.665              |
| 2003              | C++ Programming Language         | Bjarne Stroustrup     | C++ Programming        | 987.213              |
| 3001              | Java Complete Reference          | Herbert Schildt       | Java Programming       | 525.121              |
| 3002              | Core Java Volume I               | Cay Horstmann         | Java Programming       | 575.651              |
| 3003              | Java Programming Language        | James Gosling         | Java Programming       | 458.238              |
| 4001              | Operatig System Concepts         | Peter Galvin          | Operating Systems      | 567.391              |
| 4002              | Design of UNIX Operating System  | Mauris J Bach         | Operating Systems      | 421.938              |
| 4003              | UNIX Internals                   | Uresh Vahalia         | Operating Systems      | 352.822              |
+-------------------+----------------------------------+-----------------------+------------------------+----------------------+
17 rows selected (0.194 seconds)

7. Insert "new" records from books_staging into books_orc.

-- this is possible when books_orc deleted then inserting into it updated data not 
insert into books_orc                     -- books_orc need to be empty otherwise data get duplicate
select * from books_staging;

insert into books_orc                     
select * from books_staging_2;

-- inserting without deleting books_orc
insert overwrite table books_orc select * from books_staging where id not in(select id from books_orc);


insert into books_orc
select * from books_staging bs left join books_orc bo on bs.id =  

select * from books_orc;
+---------------+----------------------------------+--------------------+--------------------+------------------+
| books_orc.id  |          books_orc.name          |  books_orc.author  | books_orc.subject  | books_orc.price  |
+---------------+----------------------------------+--------------------+--------------------+------------------+
| 1001          | Exploring C                      | Yashwant Kanetkar  | C Programming      | 123.456          |
| 1002          | Pointers in C                    | Yashwant Kanetkar  | C Programming      | 371.019          |
| 1003          | ANSI C Programming               | E Balaguruswami    | C Programming      | 334.215          |
| 1004          | ANSI C Programming               | Dennis Ritchie     | C Programming      | 140.121          |
| 2001          | C++ Complete Reference           | Herbert Schildt    | C++ Programming    | 417.764          |
| 2002          | C++ Primer                       | Stanley Lippman    | C++ Programming    | 620.665          |
| 2003          | C++ Programming Language         | Bjarne Stroustrup  | C++ Programming    | 987.213          |
| 3001          | Java Complete Reference          | Herbert Schildt    | Java Programming   | 525.121          |
| 3002          | Core Java Volume I               | Cay Horstmann      | Java Programming   | 575.651          |
| 3003          | Java Programming Language        | James Gosling      | Java Programming   | 458.238          |
| 4001          | Operatig System Concepts         | Peter Galvin       | Operating Systems  | 567.391          |
| 4002          | Design of UNIX Operating System  | Mauris J Bach      | Operating Systems  | 421.938          |
| 4003          | UNIX Internals                   | Uresh Vahalia      | Operating Systems  | 352.822          |
| 20            | Atlas Shrugged                   | Ayn Rand           | Novel              | 723.9            |
| 21            | The Fountainhead                 | Ayn Rand           | Novel              | 923.8            |
| 22            | The Archer                       | Paulo Cohelo       | Novel              | 623.94           |
| 23            | The Alchemist                    | Paulo Cohelo       | Novel              | 634.8            |
+---------------+----------------------------------+--------------------+--------------------+------------------+
17 rows selected (0.144 seconds)

8. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new books visible in report?

select * from subjectwise_avg_price;
+--------------------------------+--------------------------------------+
| subjectwise_avg_price.subject  | subjectwise_avg_price.average_price  |
+--------------------------------+--------------------------------------+
| C++ Programming                | 675.21                               |
| Java Programming               | 519.67                               |
| Operating Systems              | 447.38                               |
| C Programming                  | 242.2                                |
+--------------------------------+--------------------------------------+
4 rows selected (0.177 seconds)

Are new books visible in report?
Ans :- No, Beacuase materialized view is permanently storage as data added to main table deos not affect materialized view

To change new records in materialized view 
    -- we need to rebuilt the materialized view

9. Rebuild the materialized view.

ALTER MATERIALIZED VIEW subjectwise_avg_price REBUILD;
-- materialized view gets rebuilt


10. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new books visible in report?

select * from subjectwise_avg_price;
+--------------------------------+--------------------------------------+
| subjectwise_avg_price.subject  | subjectwise_avg_price.average_price  |
+--------------------------------+--------------------------------------+
| Novel                          | 726.61                               |
| C++ Programming                | 675.21                               |
| Java Programming               | 519.67                               |
| Operating Systems              | 447.38                               |
| C Programming                  | 242.2                                |
+--------------------------------+--------------------------------------+
5 rows selected (0.143 seconds)

Are new books visible in report?
==> Yes, materialized view gets rebuilt

11. Increase price of all Java books by 10% in books_orc.

update books_orc set price = price * 1.1 where subject rlike '.*java.*';

update books_orc set price = price *1.1 where subject = "Java Programming";

12. Rebuild the materialized view.

ALTER materialized view subjectwise_avg_price REBUILD;


13. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new price changes visible in report?

select * from subjectwise_avg_price;

14. Delete all Java books.

delete from books_orc where name rlike '.*java.*';

15. Rebuild the materialized view.

ALTER materialized view subjectwise_avg_price REBUILD;

16. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new price changes visible in report?

select * from subjectwise_avg_price;


5.
1.all commands on terminal
#first create the directory in hdfs
hadoop fs -mkdir -p /user/$USER/movies/input

# then put data into the directory
hadoop fs -put /home/sunbeam/BigData/data/movies/movies_caret.csv /user/$USER/movies/input

# check the content by using this command and also check if the file is created on th server
hadoop fs -cat /user/$USER/movies/input/movies_caret.csv

2. drop table if exists movies1
create external table movies1
(id int, title string, genres array<string>) 
row format delimited
 fields terminated by '^'
  stored as textfile
   location '/user/sunbeam/movies/input';
   
select count(id) from movies1 where array_contains(genres, 'Action');
+------+
| _c0  |
+------+
| 39   |
+------+
1 row selected (24.923 seconds)


3. create external table movies2
(id int, title string, genres array<string>) 
row format delimited
 fields terminated by '^'
  stored as textfile
   location '/user/sunbeam/movies/input';
select count(id) from movies2 where size(genres) = 1;
+-------+
|  _c0  |
+-------+
| 9125  |
+-------+

16. cat > busstops.json
copy the content and paste it 
upload the data
hdfs dfs -put busstops.json /user/hive/warehouse/busstops
CREATE EXTERNAL TABLE busstops (
  `_id` STRUCT<`$oid`:STRING>,
  stop STRING,
  code STRING,
  seq FLOAT,
  stage FLOAT,
  name STRING,
  location STRUCT<type:STRING,coordinates:ARRAY<FLOAT>>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hive/warehouse/busstops';
  
