select * from fact_ticket
select * from dim_film
select * from dim_customer

-- Group by Film to analyze number_order, revenue
with table1 AS ( select film
   , count(orderid) as number_order
   , sum(total) as revenue
from fact_ticket
group by film )
SELECT *
   , (select sum(number_order) from table1) as total_order
   , (select sum(revenue) from table1) as total_revenue
FROM table1

-- Calculate percentage of each films/total
with table_1 as (
   select distinct film
       , count(orderid) over ( PARTITION BY film ) as film_order
       , sum(total) over (PARTITION by film) as film_revenue
   from fact_ticket
)
, table_2 as (
   SELECT *
       , (select sum(film_order) from table_1) as total_order
       , (select sum(cast(film_revenue as float)) from table_1) as total_revenue
   from table_1
)
select *
   , cast((CAST(film_order as float)/total_order) as decimal(10,2)) as pct_order
   , cast((CAST(film_revenue as float)/total_revenue) as decimal(10,2)) as pct_revenue
from table_2

-- Calculate percentage order,revenue by day
with table_1 as (
   select RIGHT(fact.[date], 2) AS day
       , COUNT(orderid) as number_order
       , SUM(total) as revenue
   from fact_ticket as fact
   left join dim_film as film
       on fact.film = film.film
   LEFT join dim_customer as cus
       on fact.customerid = cus.customerid
   GROUP by RIGHT(fact.[date], 2)
)
, table_2 as (
   select *
       , (select sum(number_order) from table_1) as total_order
       , (select sum(CAST(revenue as float)) from table_1) as total_revenue
   from table_1
)
SELECT *
   , cast(CAST(number_order as float)/total_order AS decimal(10,2)) as pct_order
   , cast(CAST(revenue as float)/total_revenue AS decimal(10,2)) as pct_revenue
FROM table_2
ORDER BY [day] ASC

-- Calculate percentage order,revenue by slot_type
with table_1 as (
   select slot_type
       , count( orderid) as number_order
       , sum(cast(total as float)) as revenue
   from fact_ticket as fact
   left join dim_film as film
       on fact.film = film.film
   LEFT join dim_customer as cus
       on fact.customerid = cus.customerid
   group by slot_type
)
, table_2 as(
   SELECT *
       , (select sum(number_order) from table_1) as total_order
       , (select sum(CAST(revenue as float)) from table_1) as total_revenue
   from table_1
)
SELECT *
   , cast(CAST(number_order as float)/total_order AS decimal(10,2)) as pct_order
   , cast(CAST(revenue as float)/total_revenue AS decimal(10,2)) as pct_revenue
FROM table_2

-- Using Pivot table to distribute number_order each film by day
with table_1 as (
   select RIGHT(fact.[date], 2) AS [day]
       , fact.film as film
       , count( orderid) as number_order
       , sum(cast(total as float)) as revenue
   from fact_ticket as fact
   left join dim_film as film
       on fact.film = film.film
   LEFT join dim_customer as cus
       on fact.customerid = cus.customerid
   group by RIGHT(fact.[date], 2), fact.film
)
, table_2 as(
   SELECT *
       , sum(number_order) over (partition by film) as total_order
       , sum(CAST(revenue as float)) over (partition by film) as total_revenue
   from table_1
)
, table_3 as (
   SELECT *
       , cast(CAST(number_order as float)/total_order AS decimal(10,2)) as pct_order
       , cast(CAST(revenue as float)/total_revenue AS decimal(10,2)) as pct_revenue
   FROM table_2
--order by film, [day]
)
select film
   , "01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
from (select film, number_order,[day] from table_3 ) as table_4
PIVOT (
SUM (number_order)
 FOR [day] IN ( "01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31")
) AS logic_pivot

-- Count average orders/times
with table_1 as(
   select distinct fact.film
       , room
       , fact.[date]
       , [time]
   from fact_ticket as fact
   left join dim_film as film
       on fact.film = film.film
   LEFT join dim_customer as cus
       on fact.customerid = cus.customerid
   --order by fact.film
)
, table_2 as (   
   select distinct film
       , count([date]) over (partition by film) as number_times
       , (select count([date]) from table_1) as total_times
   from table_1
)
, table_3 as (
   select fact.film
       , count(orderid) as number_order
   from fact_ticket as fact
   left join dim_film as film
       on fact.film = film.film
   LEFT join dim_customer as cus
       on fact.customerid = cus.customerid
   group by fact.film
)
select table_2.film, number_order, number_times, total_times
   , cast( cast(number_times as float)/ total_times as decimal(10,3)) as pct
   , cast( cast(number_order as float)/number_times as decimal(10,2)) as avg_order_per_times
from table_2
full join table_3
   on table_2.film = table_3.film
order by avg_order_per_times ASC

-- Order/times by day
with table_1 as(
   select distinct fact.film
       , room
       , fact.[date]
       , [time]
   from fact_ticket as fact
   left join dim_film as film
       on fact.film = film.film
   LEFT join dim_customer as cus
       on fact.customerid = cus.customerid
   --order by fact.film
)
, table_2 as (   
   select distinct right([date], 2) as [day]
       , count([date]) over (partition by right([date], 2)) as number_times
       , (select count([date]) from table_1) as total_times
   from table_1
)
, table_3 as (
   select right(fact.[date], 2) as [day]
       , count(orderid) as number_order
   from fact_ticket as fact
   left join dim_film as film
       on fact.film = film.film
   LEFT join dim_customer as cus
       on fact.customerid = cus.customerid
   group by right(fact.[date], 2)
)
select table_2.[day], number_order, number_times, total_times
   ,cast( cast(number_times as float)/ total_times as decimal(10,3)) as pct
   , cast( cast(number_order as float)/number_times as decimal(10,2)) as avg_order_per_times
from table_2
full join table_3
   on table_2.[day] = table_3.[day]
order by avg_order_per_times ASC

-- Using Pivot table to find number of times each film by day
with table_1 as(
  select distinct fact.film
      , room
      , fact.[date]
      , [time]
  from fact_ticket as fact
  left join dim_film as film
      on fact.film = film.film
  LEFT join dim_customer as cus
      on fact.customerid = cus.customerid
  --order by fact.film
)
, table_2 as (  
  select distinct film, right([date], 2) as [day]
      , count([date]) over (partition by film, right([date], 2)) as number_times
  from table_1
  --order by [film], [day]
)
, table_3 as (
  select distinct fact.film
      , right(fact.[date], 2) as [day]
      , count(orderid) as number_order
  from fact_ticket as fact
  left join dim_film as film
      on fact.film = film.film
  LEFT join dim_customer as cus
      on fact.customerid = cus.customerid
  group by fact.film, right(fact.[date], 2)
  --order by [film], [day]
)
, table_4 as (
  select distinct table_2.film, table_2.day, number_times
  from table_2
  left join table_3
      on table_2.[film] = table_3.[film]
  --order by film, day ASC
)
select film
  , "01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
from (select film, number_times,[day] from table_4 ) as table_5
PIVOT (
SUM (number_times)
FOR [day] IN ( "01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31")
) AS logic_pivot

-- Find number_order each films by room
with table_1 as (
   SELECT fact.film
   , room
   , COUNT(distinct orderid) AS number_order
FROM fact_ticket as fact
LEFT JOIN dim_film AS film
   ON fact.film = film.film
LEFT JOIN dim_customer AS cus
   ON fact.customerid = cus.customerid
GROUP by fact.film, room
-- ORDER BY film
)
select film
  , "1" , "2", "3", "4"
from (select film, number_order,[room] from table_1 ) as table_2
PIVOT (
SUM (number_order)
FOR [room] IN ( "1" , "2", "3", "4")
) AS logic_pivot

-- Using Pivot table to find number_order each age
WITH table_1 as (
   SELECT fact.film, (2019-CAST(LEFT(DOB, 4) as float)) AS age
       , count( orderid) AS number_order
   FROM fact_ticket as fact
   LEFT JOIN dim_film AS film
       ON fact.film = film.film
   LEFT JOIN dim_customer AS cus
       ON fact.customerid = cus.customerid
   where (2019-CAST(LEFT(DOB, 4) as float)) > 3 AND fact.customerid NOT in ('KH6166700', '0000029127','0001121703' )
   GROUP by fact.film, (2019-CAST(LEFT(DOB, 4) as float))
   --ORDER BY fact.film, age
)
, table_2 as (
   SELECT *
   , SUM(number_order) OVER (PARTITION BY film) as film_order
   FROM table_1
)
, table_3 as (
   SELECT *
       , cast(CAST(number_order as float)/ film_order as decimal(10,2)) as pct_age
   FROM table_2
   --ORDER BY film, age
)
select film
  , "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50"
from (select film, number_order,[age] from table_3 ) as table_4
PIVOT (
SUM (number_order)
FOR [age] IN ( "02", "03", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50")
) AS logic_pivot

-- Using Pivot table to find number_times each cashiers in charge
with table_1 as (
   SELECT distinct cashier, right(sales_date_extracted,2) AS [day], left(sales_time_extracted, 2) as [time]
   FROM fact_ticket as fact
   LEFT JOIN dim_film AS film
       ON fact.film = film.film
   LEFT JOIN dim_customer AS cus
       ON fact.customerid = cus.customerid
   --ORDER BY cashier,[day]
)
, table_2 as (
   SELECT distinct cashier, [day]
       , COUNT([time]) OVER (PARTITION by cashier, [day]) as working_time
   FROM table_1
   --ORDER BY cashier, [day]
)
select cashier
   , "01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
from (select cashier, working_time,[day] from table_2 ) as table_3
PIVOT (
 SUM (working_time)
 FOR [day] IN ( "01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31")
) AS logic_pivot

-- Cohort analysis by drawing heat map
WITH table_1 AS (
  SELECT fact.customerid , orderid, fact.date
      , first_day = MIN (DATEPART(DAY,fact.date ) ) OVER (PARTITION BY fact.customerid)
   FROM fact_ticket as fact
   LEFT JOIN dim_film AS film
       ON fact.film = film.film
   LEFT JOIN dim_customer AS cus
       ON fact.customerid = cus.customerid
   WHERE fact.customerid not in ('KH6166700', '0000029127','0001121703' )
)
, table_sub AS (
  SELECT *
      , DATEPART(DAY,[date] ) - first_day AS sub_day
  FROM table_1
)
, table_all AS (
  SELECT first_day AS acquisition_day
      , sub_day AS subsequent_day
      , COUNT (distinct customerid) AS retained_customers
  FROM table_sub
  GROUP BY first_day, sub_day
  -- ORDER BY acquisition_day, subsequent_day
)
, table_rentention AS (
  SELECT *
      , original_customers = MAX (retained_customers ) OVER ( PARTITION BY acquisition_day)
      , CAST ( retained_customers AS DECIMAL) / MAX (retained_customers ) OVER ( PARTITION BY acquisition_day) AS pct
  FROM table_all
)
SELECT acquisition_day, original_customers
  , "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", subsequent_day
FROM (
  SELECT acquisition_day, subsequent_day, original_customers, CAST ( pct AS DECIMAL (10,2) ) AS pct
  FROM table_rentention
  ) AS source_table
PIVOT (
 SUM(pct)
 FOR subsequent_day IN ( "0", "1", "2", "3","4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30")
) AS logic_pivot
ORDER BY acquisition_day

-- Analyze the problems of 3 last weekdays
with table_1 as (
select datepart(weekday,fact.[date]) as [weekday]
   , right(fact.[date], 2) as [day]
   , fact.orderid
   , ticket_price
FROM fact_ticket as fact
LEFT JOIN dim_film AS film
   ON fact.film = film.film
LEFT JOIN dim_customer AS cus
   ON fact.customerid = cus.customerid
where (datepart(weekday,fact.[date]) ='5' or datepart(weekday,fact.[date])='6' or datepart(weekday,fact.[date])='7')
and right(fact.[date], 2) not like '03'
)
, table_2 as (
SELECT [weekday]
   , SUM(ticket_price) as total
   , COUNT( orderid) as num_order
   , COUNT(distinct [day]) as num_day
FROM table_1
GROUP by [weekday]
)
select [weekday] + 1 as [weekday]
   , CAST(total as float)/num_day as avg_revenue
   , cast(CAST(num_order as float)/ num_day AS DECIMAL(10,0)) as avg_order
from table_2








