/*
  Post ETL review.  Compares the statistics from the current etl to the prior.  Use as a
  quick check from one ETL to the next.
  
  PreCondition:
	  See post_etl_stats.sql for definition of <schema_name>.dataSet_name()
	  
	  At least two set of post_etl_stats run for the same dataset name.
  
  PostCondition:
  
	A set of queries results that show the difference between the current etl_stats and the prior.
  

  Change Log
  Who		When		What
  dtk		19Feb15		Added previous date to queries
  dtk		15May2015		Update query showing percent mapped
  dtk		19Jun2016		Do side by side comparison
  dtk		13Sep2016		Rename schema_name and db_name to dataSetName
  dtk		13Sept2016	Compare event types
  dtk		16Sept2016	Rename to cdm_stats
  dtk		21Oct2016		Added schema name dtorok to dataSet_name()
 */
 
 -- Compare row counts to previous ETLs (1)
 WITH counts( table_name, description, value, log_date, rowNumber )
AS
(
select table_name, description_1,  CAST( REPLACE( value_1, ',', '' ) AS BIGINT ) AS value, log_date
     , row_number() OVER( partition by table_name order by log_date DESC) as rowNumber
 from <schema_name>.cdm_stats 
where dataSet_name = <schema_name>.dataSet_name() and METRIC_NAME = 'count'  
)
SELECT last.table_name, last.description, last.log_date as last_date, prior.log_date as prior_date
      ,<schema_name>.f_formatNumber(last.value) last_count, <schema_name>.f_formatNumber(prior.value) prior_count
      ,<schema_name>.f_formatNumber(CAST( round( last.value - prior.value ) AS bigInt ) )as diff
      , CASE WHEN last.value <> 0 
                 THEN ((CAST(last.value AS decimal) - prior.value)/last.value) * 100 
		 ELSE NULL
	END as perc
FROM
(
select table_name, description, value, log_date, rownumber
from counts 
WHERE rownumber = 1
) last
JOIN
(
select table_name, description, value, log_date, rownumber
from counts 
WHERE rownumber = 2
) prior ON prior.table_name = last.table_name and prior.description = last.description
order by table_name;

-- Compare event_type, row count to previous (2)
 WITH counts( table_name, description, value, log_date, rowNumber )
AS
(
select table_name, description_1,  CAST( REPLACE( value_1, ',', '' ) AS BIGINT ) AS value, log_date
     , row_number() OVER( partition by table_name, description_1 order by log_date DESC) as rowNumber
 from <schema_name>.cdm_stats 
where dataSet_name = <schema_name>.dataSet_name() and METRIC_NAME = 'event type'  
)
SELECT last.table_name, last.description, last.log_date as last_date, prior.log_date as prior_date
      ,<schema_name>.f_formatNumber(last.value) last_count, <schema_name>.f_formatNumber(prior.value) prior_count
      ,<schema_name>.f_formatNumber(CAST( round( last.value - prior.value ) AS bigInt ) )as diff
      , CASE WHEN last.value <> 0 
                 THEN ((CAST(last.value AS decimal) - prior.value)/last.value) * 100 
		 ELSE NULL
	END as perc
FROM
(
select table_name, description, value, log_date, rownumber
from counts 
WHERE rownumber = 1
) last
JOIN
(
select table_name, description, value, log_date, rownumber
from counts 
WHERE rownumber = 2
) prior ON prior.table_name = last.table_name and prior.description = last.description
order by table_name, description;

-- Compare events by year to previous ETLs (3)
WITH counts( table_name, log_date, description_1, year, description_2, rows, rowNumber )
AS
(
select table_name, log_date, description_1,  value_1, description_2, CAST( REPLACE( value_2, ',', '' ) AS BIGINT ) AS rows
     , row_number() OVER( partition by table_name, value_1 order by date_trunc( 'day', log_date ) DESC, value_1 DESC) as rowNumber
 from <schema_name>.cdm_stats 
where dataSet_name = <schema_name>.dataSet_name() and METRIC_NAME = 'events_by_year'  
)
SELECT last.table_name, last.log_date as last_date, prior.log_date as prior_date
      , last.description_1, last.year
      , last.description_2, <schema_name>.f_formatNumber(last.rows) AS last_rows, <schema_name>.f_formatNumber(prior.rows) as prior_rows
      , <schema_name>.f_formatNumber(CAST( round( last.rows - prior.rows ) AS bigInt ) )as diff
      , CASE WHEN last.rows <> 0 
                THEN ((CAST(last.rows AS decimal) - prior.rows)/last.rows) * 100 
		ELSE NULL
	END as perc
FROM
(
select table_name, log_date, rownumber
     , description_1, year, description_2, rows
from counts 
WHERE rownumber = 1
) last
JOIN
(
select table_name, log_date, rownumber
     , description_1, year, description_2, rows
FROM counts
WHERE rownumber = 2
) prior ON prior.table_name = last.table_name 
        and prior.description_1 = last.description_1 and prior.year = last.year
        AND prior.description_2=last.description_2
ORDER by table_name, year desc;


-- Compare min max dates to previous ETLs (4)
WITH counts( table_name, log_date, description_1,  min_date,  description_2,  max_date, rowNumber )
AS
(
select table_name, log_date, description_1, CAST(value_1 AS DATE ), description_2, CAST(value_2 AS DATE)
     , row_number() OVER( partition by table_name order by log_date DESC) as rowNumber
 from <schema_name>.cdm_stats 
where dataSet_name = <schema_name>.dataSet_name() and METRIC_NAME = 'min_max_date'   
)
SELECT last.table_name
     , last.log_date as last_date, prior.log_date as prior_date
     , last.description_1, last.min_date as last_min, prior.min_date as prior_min, dateDiff(days, prior.min_date, last.min_date ) as diff_min
     , last.description_2, last.max_date as last_max, prior.max_date as prior_max, dateDiff(days, prior.max_date, last.max_date ) as diff_max
FROM
   (
     SELECT table_name, log_date, description_1, min_date, description_2, max_date
     FROM counts
     WHERE rownumber = 1
   ) last
JOIN
  (
     SELECT table_name, log_date, description_1, min_date, description_2, max_date
     FROM counts
     WHERE rownumber = 2
   ) prior ON  prior.table_name = last.table_name and prior.description_1 = last.description_1
   order by table_name;
   
   -- Compare percent mapped to vocabulary concept (5)
   WITH counts( table_name, metric_name, log_date, description_1, items, description_2, mapped, rowNumber )
AS
(
select table_name, metric_name, log_date, description_1, CAST( REPLACE( value_1, ',', '' ) AS BIGINT ) AS items
     , description_2, CAST(value_2 AS decimal(5,2) ) AS mapped
     , row_number() OVER( partition by table_name, metric_name order by log_date DESC) as rowNumber
 from <schema_name>.cdm_stats 
where dataSet_name = <schema_name>.dataSet_name() and METRIC_NAME IN( 'concept_events_mapped', 'concepts_mapped' )
)
SELECT last.table_name, last.metric_name
      , last.log_date as last_date, prior.log_date as prior_date
      , <schema_name>.f_formatNumber(last.items) as last_count, <schema_name>.f_formatNumber(prior.items) as prior_count
      ,last.mapped as last_map, prior.mapped AS prior_map, last.mapped - prior.mapped as diff
FROM
(
select table_name, metric_name, items, log_date, mapped, rownumber
from counts 
WHERE rownumber = 1
) last
JOIN
(
select table_name, metric_name, items, log_date, mapped, rownumber
from counts 
WHERE rownumber = 2
) prior ON prior.table_name = last.table_name and prior.metric_name = last.metric_name
ORDER BY table_name, metric_name desc;
   