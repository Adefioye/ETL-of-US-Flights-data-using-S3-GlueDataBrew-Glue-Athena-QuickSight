/* Number of flights */
------------------------------------

CREATE TABLE number_of_flights
WITH (
format = 'PARQUET',
external_location = 's3://us-flight/curated/us-airlines-athena-query-results/number_of_flights'
) 
AS 
SELECT description, COUNT(description) AS number_of_flights
FROM parquet
GROUP BY description
-------------------------------

/* On-time performance */
----------------------------------

CREATE TABLE on_time_peformance_of_airlines
WITH (
format = 'PARQUET',
external_location = 's3://us-flight/curated/us-airlines-athena-query-results/on_time_peformance_of_airlines'
) 
AS 
SELECT DISTINCT description, ((1 - (num_of_delayed_flights/ total_number_of_flights)) * 100) AS on_time_performance
FROM (SELECT description,
		SUM(dep_del15) OVER(PARTITION BY description) AS num_of_delayed_flights,
		COUNT(description) OVER(PARTITION BY description) AS total_number_of_flights
	  FROM parquet) AS derived_table
-----------------------------------------------------
/* Total distance travelled by airlines */
----------------------------------------

CREATE TABLE distance_by_airline
WITH (
format = 'PARQUET',
external_location = 's3://us-flight/curated/us-airlines-athena-query-results/distance_by_airline'
) 
AS 
SELECT description, SUM(distance) AS total_distance
FROM parquet
GROUP BY description

--------------------------------------------------------
/* Percentage of flight cancellations */
---------------------------------------------

CREATE TABLE pct_cancelled_flights_by_airline
WITH (
format = 'PARQUET',
external_location = 's3://us-flight/curated/us-airlines-athena-query-results/pct_cancelled_flights_by_airline'
) 
AS 
SELECT DISTINCT description, ((1 - (num_of_cancelled_flights / total_number_of_flights)) * 100) AS pct_cancelled_flight
FROM (SELECT description,
		SUM(cancelled) OVER(PARTITION BY description) AS num_of_cancelled_flights,
		COUNT(description) OVER(PARTITION BY description) AS total_number_of_flights
	  FROM parquet) AS derived_table
-------------------------------------------------------------
/* Which month has the highest number of flights? */
--------------------------------------------------

CREATE TABLE num_of_flights_by_month
WITH (
format = 'PARQUET',
external_location = 's3://us-flight/curated/us-airlines-athena-query-results/num_of_flights_by_month'
) 
AS 
SELECT CASE WHEN month = 1 THEN 'January'
	        WHEN month = 2 THEN 'February'
	        WHEN month = 3 THEN 'March'
	        WHEN month = 4 THEN 'April'
	        WHEN month = 5 THEN 'May'
	        WHEN month = 6 THEN 'June'
	        WHEN month = 7 THEN 'July'
	        WHEN month = 8 THEN 'August'
	        WHEN month = 9 THEN 'September'
	        WHEN month = 10 THEN 'October'
	        WHEN month = 11 THEN 'November'
	        ELSE 'December'
	   END AS month_name, number_of_flights
FROM (SELECT DISTINCT MONTH(fl_date) AS month,  
             COUNT(fl_date) OVER(PARTITION BY MONTH(fl_date)) AS number_of_flights
       FROM parquet) derived_table

---------------------------------------------------------------------------------------------
/* Which day has the highest number of flights */
----------------------------------------------
CREATE TABLE num_of_flights_by_day
WITH (
format = 'PARQUET',
external_location = 's3://us-flight/curated/us-airlines-athena-query-results/num_of_flights_by_day'
) 
AS 
SELECT CASE WHEN day = 1 THEN 'Monday'
	        WHEN day = 2 THEN 'Tuesday'
	        WHEN day = 3 THEN 'Wednesday'
	        WHEN day = 4 THEN 'Thursday'
	        WHEN day = 5 THEN 'Friday'
	        WHEN day = 6 THEN 'Saturday'
            ELSE 'Sunday'
	   END AS day_name, number_of_flights
FROM (SELECT DISTINCT day_of_week(fl_date) AS day,  
             COUNT(fl_date) OVER(PARTITION BY day_of_week(fl_date)) AS number_of_flights
       FROM parquet) derived_table


