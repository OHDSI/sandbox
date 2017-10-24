CREATE  OR REPLACE  FUNCTION <schema_name>.dataSet_name()
RETURNS varchar(30) IMMUTABLE
AS $$
#
# Use standard name for CDM sets so that compare will look at dataSet_name/date
#	e.g.
#		'CCAE'
#		'MDCR'
#		'OPTUM'
#
# 
# Change log
#	Who		When		        What
#	dtk		19Jul2016	    Created
#
return "CCAE"
$$ LANGUAGE plpythonu;

/*  Post ETL QC checks

   Preconditions:
	1) Set search path to schema with OMOP CDM of interest
	2) uses functions  <schema_name>.f_formatNumber & <schema_name>.dataSet_name()
	3) define the dataset in dataSet_name()
	4) table  <schema_name>.cdm_stats exists (see below)
	
	Change LOG
	Who		WHEN		What
	dtk		18Feb15		Made value1 varchar
	dtk           	 01Sep15         	Port To RedShift
	dtk		08Dec2016		Added CDMv5 tables
    	dtk		21 Dec2016	Fix problem in condition type where there were two different concept ids
						one concept type and the other procedure type both with the same name
 */

/*CREATE TABLE <schema_name>.cdm_stats
(
	id bigint IDENTITY(1,1) NOT NULL,
	log_date date NULL,
	dataSet_name varchar(30) NOT NULL,
	table_name varchar(30) NOT NULL,
	metric_name varchar(30) NOT NULL,
	description_1 varchar(65) NULL,
	value_1 varchar( 15 ),
	description_2 varchar(65) NULL,
	value_2 varchar( 15 )
) ;
*/


-- events_by_year by datePart(year,  date )
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'observation_period', 'events_by_year'
     , 'Year', CAST( datePart(year, OBSERVATION_PERIOD_START_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM observation_period
GROUP BY datePart(year, OBSERVATION_PERIOD_START_DATE)
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'observation_period', 'min_max_date'
     , 'Min date', CAST( min(OBSERVATION_PERIOD_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(OBSERVATION_PERIOD_END_DATE) AS varchar(12) )
from OBSERVATION_period
;
-- Payer Plan Period
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'payer_plan_period', 'events_by_year'
     , 'Year', CAST( datePart(year, payer_plan_period_START_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM payer_plan_period
GROUP BY datePart(year, payer_plan_period_START_DATE)
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'payer_plan_period', 'min_max_date'
     , 'Min date', CAST( min(payer_plan_period_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(payer_plan_period_END_DATE) AS varchar(12) )
from payer_plan_period
;
-- Visit Type
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'visit_occurrence', 'event type'
     , concept_name as visit_type, <schema_name>.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- visit types with count
( SELECT visit_type_concept_id, count(*) AS rows
    FROM visit_occurrence
   GROUP BY visit_type_concept_id
) visit_type
JOIN concept ON concept_id = visit_type_concept_id;

-- Visit min max
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'visit_occurrence', 'min_max_date'
     , 'Min date', CAST( min(visit_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(visit_END_DATE) AS varchar(12) )
from visit_occurrence
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'visit_cost', 'cost'
     , 'Total Paid (Millions)', <schema_name>.f_formatNumber( CAST( sum( total_paid )/1000000 AS INTEGER ) )
     , NULL, NULL
FROM visit_cost
;
-- Condition Occurrence
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'condition_occurrence', 'events_by_year'
     , 'Year', CAST( datePart(year, condition_START_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM condition_occurrence
GROUP BY datePart(year, condition_START_DATE)
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'condition_occurrence', 'min_max_date'
     , 'Min date', CAST( min(condition_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(condition_END_DATE) AS varchar(12) )
from condition_occurrence
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'condition_occurrence', 'event type'
     , concept_name as event_type, <schema_name>.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT concept_name, sum( rows ) as rows
    FROM -- condition type concept id
    ( SELECT condition_type_concept_id, count(*) AS rows
       FROM condition_occurrence
       GROUP BY condition_type_concept_id
    ) event_type
   JOIN concept ON concept_id = condition_type_concept_id
  GROUP BY concept_name
);

--- Procedure occurrence
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'procedure_occurrence', 'events_by_year'
     , 'Year', CAST( datePart(year, procedure_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM procedure_occurrence
GROUP BY datePart(year, procedure_DATE)
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'procedure_cost', 'cost'
     , 'Total Paid (Millions)', <schema_name>.f_formatNumber( CAST( sum( total_paid )/1000000 AS INTEGER ) )
     , NULL, NULL
FROM procedure_cost
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'procedure_occurrence', 'min_max_date'
     , 'Min date', CAST( min(procedure_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(procedure_DATE) AS varchar(12) )
from procedure_occurrence
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'procedure_occurrence', 'event type'
     , concept_name as event_type, <schema_name>.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT procedure_type_concept_id, count(*) AS rows
    FROM procedure_occurrence
   GROUP BY procedure_type_concept_id
) event_type
JOIN concept ON concept_id = procedure_type_concept_id;

-- Device
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'device_exposure', 'events_by_year'
     , 'Year', CAST( datePart(year, device_exposure_start_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM device_exposure
GROUP BY datePart(year, device_exposure_start_DATE)
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'device_exposure', 'min_max_date'
     , 'Min date', CAST( min(device_exposure_start_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(device_exposure_start_DATE) AS varchar(12) )
from device_exposure
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'device_exposure', 'event type'
     , concept_name as event_type, <schema_name>.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT device_type_concept_id, count(*) AS rows
    FROM device_exposure
   GROUP BY device_type_concept_id
) event_type
JOIN concept ON concept_id = device_type_concept_id;

--- Drug exposure
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'drug_exposure', 'events_by_year'
     , 'Year', CAST( datePart(year, drug_exposure_START_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM drug_exposure
GROUP BY datePart(year, drug_exposure_START_DATE)
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'drug_exposure', 'min_max_date'
     , 'Min date', CAST( min(drug_exposure_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(drug_exposure_END_DATE) AS varchar(12) )
from drug_exposure
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'drug_occurrence', 'event type'
     , concept_name as event_type, <schema_name>.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT drug_type_concept_id, count(*) AS rows
    FROM drug_exposure
   GROUP BY drug_type_concept_id
) event_type
JOIN concept ON concept_id = drug_type_concept_id;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'drug_cost', 'cost'
     , 'Total Paid (Millions)', <schema_name>.f_formatNumber( CAST( sum( total_paid )/1000000 AS INTEGER ) )
     , NULL, NULL
FROM drug_cost
;
--- Observation
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'observation', 'events_by_year'
     , 'Year', CAST( datePart(year, observation_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM observation
GROUP BY datePart(year, observation_DATE)
;
---
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'observation', 'min_max_date'
     , 'Min date', CAST( min(observation_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(observation_DATE) AS varchar(12) )
from observation
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'observation', 'event type'
     , concept_name as event_type, <schema_name>.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT observation_type_concept_id, count(*) AS rows
    FROM observation
   GROUP BY observation_type_concept_id
) event_type
JOIN concept ON concept_id = observation_type_concept_id;

-- Measurement
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'measurement', 'events_by_year'
     , 'Year', CAST( datePart(year, measurement_DATE) AS varchar(4) )
     , 'Rows',  <schema_name>.f_formatNumber( count(1) )
FROM measurement
GROUP BY datePart(year, measurement_DATE)
;
---
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'measurement', 'min_max_date'
     , 'Min date', CAST( min(measurement_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(measurement_DATE) AS varchar(12) )
from measurement
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), <schema_name>.dataSet_name(), 'measurement', 'event type'
     , concept_name as event_type, <schema_name>.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT measurement_type_concept_id, count(*) AS rows
    FROM measurement
   GROUP BY measurement_type_concept_id
) event_type
JOIN concept ON concept_id = measurement_type_concept_id;


---- counts ---------------------

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'care_site', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM care_site
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'condition_occurrence', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM condition_occurrence
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'death', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM death
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'drug_era', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM drug_era
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'location', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM location
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'observation', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM observation
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'measurement', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM measurement
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'observation_period', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM observation_period
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'note', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM note
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'payer_plan_period', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM payer_plan_period
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'person', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM person
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'procedure_occurrence', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM procedure_occurrence
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'device_exposure', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM device_exposure
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'device_cost', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM device_cost
;

INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'procedure_cost', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM procedure_cost
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'drug_exposure', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM drug_exposure
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'drug_cost', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM drug_cost
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'provider', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM provider
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'specimen', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM specimen
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'visit_occurrence', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM visit_occurrence
;
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), <schema_name>.dataSet_name(), 'visit_cost', 'count'
     , 'Rows', <schema_name>.f_formatNumber( count(1) )
 FROM visit_cost
;

--- Concept Mapping
-- Drug Exposure
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), <schema_name>.dataSet_name(), 'drug_exposure', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN <schema_name>.f_formatNumber( distinct_drugs )
	               WHEN 'concept_events_mapped' THEN <schema_name>.f_formatNumber( events )
	   END
	 , 'percent mapped'
	 , CASE metric WHEN 'concepts_mapped' THEN percent_drugs_mapped
	               WHEN 'concept_events_mapped' THEN percent_events_mapped
	   END
FROM
(
SELECT COUNT(*) AS distinct_drugs
	 , CAST( CAST( 100 * CAST( SUM(mapped) as decimal(18,0) ) / COUNT(*) AS decimal(4,1) ) AS varchar(5))
	   AS percent_drugs_mapped
	 , SUM( events ) as events
	 , CAST( CAST( 100 * CAST( SUM( MAPPED * events ) AS decimal(18, 0 )) / SUM(events) AS decimal(4,1)) AS varchar(5))
	  AS percent_events_mapped
  FROM -- unique drug with count and mapping
     (  SELECT drug_source_value
             , CASE WHEN drug_concept_id > 0 THEN 1 ELSE 0 END AS MAPPED
	         , COUNT(*) as events
		 FROM drug_exposure
         GROUP BY drug_source_value
             , CASE WHEN drug_concept_id > 0 THEN 1 ELSE 0 END
	 ) drug_map
) drug_concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;
-- Condition Occurrence
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), <schema_name>.dataSet_name(), 'condition_occurrence', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN <schema_name>.f_formatNumber( distinct_conditions )
	               WHEN 'concept_events_mapped' THEN <schema_name>.f_formatNumber( events )
	   END
	 , 'percent mapped'
	 , CASE metric WHEN 'concepts_mapped' THEN percent_conditions_mapped
	               WHEN 'concept_events_mapped' THEN percent_events_mapped
	   END
FROM
(
SELECT COUNT(*) AS distinct_conditions
	 , CAST( CAST( 100 * CAST( SUM(mapped) as decimal(18,0) ) / COUNT(*) AS decimal(4,1) ) AS varchar(5))
	   AS percent_conditions_mapped
	 , SUM( events ) as events
	 , CAST( CAST( 100 * CAST( SUM( MAPPED * events ) AS decimal(18, 0 )) / SUM(events) AS decimal(4,1)) AS varchar(5))
	  AS percent_events_mapped
  FROM -- unique conditions with count and mapping
     (  SELECT condition_source_value
             , CASE WHEN condition_concept_id > 0 THEN 1 ELSE 0 END AS MAPPED
	         , COUNT(*) as events
		 FROM condition_occurrence
         GROUP BY condition_source_value
             , CASE WHEN condition_concept_id > 0 THEN 1 ELSE 0 END
	 ) map
) concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;

-- Procedures
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), <schema_name>.dataSet_name(), 'procedure_occurrence', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN <schema_name>.f_formatNumber( distinct_procedures )
	               WHEN 'concept_events_mapped' THEN <schema_name>.f_formatNumber( events )
	   END
	 , 'percent mapped'
	 , CASE metric WHEN 'concepts_mapped' THEN percent_procedures_mapped
	               WHEN 'concept_events_mapped' THEN percent_events_mapped
	   END
FROM
(
SELECT COUNT(*) AS distinct_procedures
	 , CAST( CAST( 100 * CAST( SUM(mapped) as decimal(18,0) ) / COUNT(*) AS decimal(4,1) ) AS varchar(5))
	   AS percent_procedures_mapped
	 , SUM( events ) as events
	 , CAST( CAST( 100 * CAST( SUM( MAPPED * events ) AS decimal(18, 0 )) / SUM(events) AS decimal(4,1)) AS varchar(5))
	  AS percent_events_mapped
  FROM -- unique procedures with count and mapping
     (  SELECT procedure_source_value
             , CASE WHEN procedure_concept_id > 0 THEN 1 ELSE 0 END AS MAPPED
	         , COUNT(*) as events
		 FROM procedure_occurrence
         GROUP BY procedure_source_value
             , CASE WHEN procedure_concept_id > 0 THEN 1 ELSE 0 END
	 ) map
) concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;


-- Observation
INSERT INTO <schema_name>.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), <schema_name>.dataSet_name(), 'observation', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN <schema_name>.f_formatNumber( distinct_procedures )
	               WHEN 'concept_events_mapped' THEN <schema_name>.f_formatNumber( events )
	   END
	 , 'percent mapped'
	 , CASE metric WHEN 'concepts_mapped' THEN percent_procedures_mapped
	               WHEN 'concept_events_mapped' THEN percent_events_mapped
	   END
FROM
(
SELECT COUNT(*) AS distinct_procedures
	 , CAST( CAST( 100 * CAST( SUM(mapped) as decimal(18,0) ) / COUNT(*) AS decimal(4,1) ) AS varchar(5))
	   AS percent_procedures_mapped
	 , SUM( events ) as events
	 , CAST( CAST( 100 * CAST( SUM( MAPPED * events ) AS decimal(18, 0 )) / SUM(events) AS decimal(4,1)) AS varchar(5))
	  AS percent_events_mapped
  FROM -- unique observations with count and mapping
     (  SELECT observation_source_value
             , CASE WHEN observation_concept_id > 0 THEN 1 ELSE 0 END AS MAPPED
	         , COUNT(*) as events
		 FROM observation
         GROUP BY observation_source_value
             , CASE WHEN observation_concept_id > 0 THEN 1 ELSE 0 END
	 ) map
) concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;



