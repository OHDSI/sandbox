Set search_path to clinformatics_cdm5_02_17_q2_bms;

CREATE  OR REPLACE  FUNCTION dtorok.dataSet_name()
RETURNS varchar(30) IMMUTABLE
AS $$
#
# Use standard name for CDM sets so that compare will look at dataSet_name/date
#	e.g.
#		'OPTUM Clinformatics'
#		'Celgene MDCR'
#		'Celgene CCAE'
#		'Celgene Truven Combined Qtrly'
#		'truven_monthly_amgen_v5'
#              'combined_quarterly_bms_v5'
#		'Pharmetrics Plus v5'
#
# 
# Change log
#	Who		When		        What
#	dtk		19Jul2016	    Created
#
return "OPTUM Clinformatics"
$$ LANGUAGE plpythonu;

/*  Post ETL QC checks

   Preconditions:
	1) Set search path to OMOP CDM
	2) uses function  dtorok.f_formatNumber & dtorok.dataSet_name()
	
	Change LOG
	Who		WHEN		What
	dtk		18Feb15		Made value1 varchar
	dtk           	 01Sep15         	Port To RedShift
	dtk		08Dec2016		Added CDMv5 tables
    	dtk		21 Dec2016	Fix problem in condition type where there were two different concept ids
						one concept type and the other procedure type both with the same name
	dtk		22Dec2017		COALESCE( sum( total_paid), 0 )
	dtk    24Jan2018     Add top ten concept
 */

/*CREATE TABLE dtorok.cdm_stats
(
	id bigint IDENTITY(1,1) NOT NULL,
	log_date date NULL,
	dataSet_name varchar(30) NOT NULL,
	table_name varchar(30) NOT NULL,
	metric_name varchar(30) NOT NULL,
	value_1 varchar( 15 ),
	value_2 varchar( 15 ),
	description_1 varchar(65) NULL,
	description_2 varchar(65) NULL
) ;
*/


-- events_by_year by datePart(year,  date )
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'observation_period', 'events_by_year'
     , 'Year', CAST( datePart(year, OBSERVATION_PERIOD_START_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_observation_period
GROUP BY datePart(year, OBSERVATION_PERIOD_START_DATE)
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'observation_period', 'min_max_date'
     , 'Min date', CAST( min(OBSERVATION_PERIOD_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(OBSERVATION_PERIOD_END_DATE) AS varchar(12) )
from v_OBSERVATION_period
;
-- Payer Plan Period
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'payer_plan_period', 'events_by_year'
     , 'Year', CAST( datePart(year, payer_plan_period_START_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_payer_plan_period
GROUP BY datePart(year, payer_plan_period_START_DATE)
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'payer_plan_period', 'min_max_date'
     , 'Min date', CAST( min(payer_plan_period_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(payer_plan_period_END_DATE) AS varchar(12) )
from v_payer_plan_period
;
-- Visit Type
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'visit_occurrence', 'event type'
     , concept_name as visit_type, dtorok.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- visit types with count
( SELECT visit_type_concept_id, count(*) AS rows
    FROM v_visit_occurrence
   GROUP BY visit_type_concept_id
) visit_type
JOIN concept ON concept_id = visit_type_concept_id;

-- Visit min max
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'visit_occurrence', 'min_max_date'
     , 'Min date', CAST( min(visit_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(visit_END_DATE) AS varchar(12) )
from v_visit_occurrence
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'visit_cost', 'cost'
     , 'Total Paid (Millions)', dtorok.f_formatNumber( CAST(COALESCE( sum( total_paid), 0 )/1000000 AS INTEGER ) )
     , NULL, NULL
FROM v_visit_cost
;
-- Condition Occurrence
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'condition_occurrence', 'events_by_year'
     , 'Year', CAST( datePart(year, condition_START_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_condition_occurrence
GROUP BY datePart(year, condition_START_DATE)
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'condition_occurrence', 'min_max_date'
     , 'Min date', CAST( min(condition_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(condition_END_DATE) AS varchar(12) )
from v_condition_occurrence
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'condition_occurrence', 'event type'
     , concept_name as event_type, dtorok.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT concept_name, sum( rows ) as rows
    FROM -- condition type concept id
    ( SELECT condition_type_concept_id, count(*) AS rows
       FROM v_condition_occurrence
       GROUP BY condition_type_concept_id
    ) event_type
   JOIN concept ON concept_id = condition_type_concept_id
  GROUP BY concept_name
);

--- Procedure occurrence
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'procedure_occurrence', 'events_by_year'
     , 'Year', CAST( datePart(year, procedure_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_procedure_occurrence
GROUP BY datePart(year, procedure_DATE)
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'procedure_cost', 'cost'
     , 'Total Paid (Millions)', dtorok.f_formatNumber( CAST(COALESCE( sum( total_paid), 0 )/1000000 AS INTEGER ) )
     , NULL, NULL
FROM v_procedure_cost
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'procedure_occurrence', 'min_max_date'
     , 'Min date', CAST( min(procedure_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(procedure_DATE) AS varchar(12) )
from v_procedure_occurrence
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'procedure_occurrence', 'event type'
     , concept_name as event_type, dtorok.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT procedure_type_concept_id, count(*) AS rows
    FROM v_procedure_occurrence
   GROUP BY procedure_type_concept_id
) event_type
JOIN concept ON concept_id = procedure_type_concept_id;

-- Device
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'device_exposure', 'events_by_year'
     , 'Year', CAST( datePart(year, device_exposure_start_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_device_exposure
GROUP BY datePart(year, device_exposure_start_DATE)
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'device_exposure', 'min_max_date'
     , 'Min date', CAST( min(device_exposure_start_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(device_exposure_start_DATE) AS varchar(12) )
from v_device_exposure
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'device_exposure', 'event type'
     , concept_name as event_type, dtorok.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT device_type_concept_id, count(*) AS rows
    FROM v_device_exposure
   GROUP BY device_type_concept_id
) event_type
JOIN concept ON concept_id = device_type_concept_id;

--- Drug exposure
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'drug_exposure', 'events_by_year'
     , 'Year', CAST( datePart(year, drug_exposure_START_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_drug_exposure
GROUP BY datePart(year, drug_exposure_START_DATE)
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'drug_exposure', 'min_max_date'
     , 'Min date', CAST( min(drug_exposure_START_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(drug_exposure_END_DATE) AS varchar(12) )
from v_drug_exposure
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'drug_occurrence', 'event type'
     , concept_name as event_type, dtorok.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT drug_type_concept_id, count(*) AS rows
    FROM v_drug_exposure
   GROUP BY drug_type_concept_id
) event_type
JOIN concept ON concept_id = drug_type_concept_id;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'drug_cost', 'cost'
     , 'Total Paid (Millions)', dtorok.f_formatNumber( CAST(COALESCE( sum( total_paid), 0 )/1000000 AS INTEGER ) )
     , NULL, NULL
FROM v_drug_cost
;
--- Observation
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'observation', 'events_by_year'
     , 'Year', CAST( datePart(year, observation_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_observation
GROUP BY datePart(year, observation_DATE)
;
---
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'observation', 'min_max_date'
     , 'Min date', CAST( min(observation_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(observation_DATE) AS varchar(12) )
from v_observation
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'observation', 'event type'
     , concept_name as event_type, dtorok.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT observation_type_concept_id, count(*) AS rows
    FROM v_observation
   GROUP BY observation_type_concept_id
) event_type
JOIN concept ON concept_id = observation_type_concept_id;

-- Measurement
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'measurement', 'events_by_year'
     , 'Year', CAST( datePart(year, measurement_DATE) AS varchar(4) )
     , 'Rows',  dtorok.f_formatNumber( count(1) )
FROM v_measurement
GROUP BY datePart(year, measurement_DATE)
;
---
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'measurement', 'min_max_date'
     , 'Min date', CAST( min(measurement_DATE) AS varchar(12) )
	 , 'Max date', CAST( max(measurement_DATE) AS varchar(12) )
from v_measurement
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getDate(), dtorok.dataSet_name(), 'measurement', 'event type'
     , concept_name as event_type, dtorok.f_formatNumber(rows)
     , 'percent' , CAST( ROUND( CAST(rows as float)/sum(rows) OVER()  * 100, 2 ) AS varchar(4) )
FROM -- type with count
( SELECT measurement_type_concept_id, count(*) AS rows
    FROM v_measurement
   GROUP BY measurement_type_concept_id
) event_type
JOIN concept ON concept_id = measurement_type_concept_id;


---- counts ---------------------

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'care_site', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_care_site
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'condition_occurrence', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_condition_occurrence
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'death', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_death
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'drug_era', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_drug_era
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'location', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_location
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'observation', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_observation
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'measurement', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_measurement
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'observation_period', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_observation_period
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'note', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_note
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'payer_plan_period', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_payer_plan_period
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'person', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_person
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'procedure_occurrence', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_procedure_occurrence
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'device_exposure', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_device_exposure
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'device_cost', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_device_cost
;

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'procedure_cost', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_procedure_cost
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'drug_exposure', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_drug_exposure
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'drug_cost', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_drug_cost
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'provider', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_provider
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'specimen', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_specimen
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'visit_occurrence', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_visit_occurrence
;
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1 )
SELECT getdate(), dtorok.dataSet_name(), 'visit_cost', 'count'
     , 'Rows', dtorok.f_formatNumber( count(1) )
 FROM v_visit_cost
;

--- Concept Mapping
-- Drug Exposure
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'drug_exposure', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN dtorok.f_formatNumber( distinct_drugs )
	               WHEN 'concept_events_mapped' THEN dtorok.f_formatNumber( events )
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
		 FROM v_drug_exposure
         GROUP BY drug_source_value
             , CASE WHEN drug_concept_id > 0 THEN 1 ELSE 0 END
	 ) drug_map
) drug_concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;
-- Condition Occurrence
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'condition_occurrence', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN dtorok.f_formatNumber( distinct_conditions )
	               WHEN 'concept_events_mapped' THEN dtorok.f_formatNumber( events )
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
		 FROM v_condition_occurrence
         GROUP BY condition_source_value
             , CASE WHEN condition_concept_id > 0 THEN 1 ELSE 0 END
	 ) map
) concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;

-- Procedures
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'procedure_occurrence', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN dtorok.f_formatNumber( distinct_procedures )
	               WHEN 'concept_events_mapped' THEN dtorok.f_formatNumber( events )
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
		 FROM v_procedure_occurrence
         GROUP BY procedure_source_value
             , CASE WHEN procedure_concept_id > 0 THEN 1 ELSE 0 END
	 ) map
) concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;


-- Observation
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'observation', metric
     , CASE metric WHEN 'concepts_mapped' THEN 'unique_concepts' 
	               WHEN 'concept_events_mapped' THEN 'events'
	   END
	 , CASE metric WHEN 'concepts_mapped' THEN dtorok.f_formatNumber( distinct_procedures )
	               WHEN 'concept_events_mapped' THEN dtorok.f_formatNumber( events )
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
		 FROM v_observation
         GROUP BY observation_source_value
             , CASE WHEN observation_concept_id > 0 THEN 1 ELSE 0 END
	 ) map
) concepts
CROSS JOIN( SELECT 'concepts_mapped' AS metric UNION SELECT 'concept_events_mapped' ) metric
;

-- Top 10 Concepts
-- Conditions
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'condition_occurrence', 'Top 10 event concepts' as metric_name
     , 'concept_name' AS description_1, subString(concept_name, 1, 30) as value_1
     , 'rows' AS description_2, dtorok.f_formatNumber(rows)
FROM
(
  SELECT concept_name, rows
  FROM -- top 10 conditions
  (
    SELECT top 10 *
    FROM -- condition_concept_id with count
      (
        SELECT condition_concept_id, count(*) AS ROWS
        FROM v_condition_occurrence
        GROUP BY 1
        ORDER BY 2 desc
      )
  ) JOIN concept ON concept_id = condition_concept_id
);
-- Drugs
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'drug_exposure', 'Top 10 event concepts' as metric_name
     , 'concept_name' AS description_1, subString(concept_name, 1, 30) as value_1
     , 'rows' AS description_2, dtorok.f_formatNumber(rows)
FROM
(
  SELECT concept_name, rows
  FROM -- top 10 
  (
    SELECT top 10 *
    FROM -- concept_id with count
      (
        SELECT drug_concept_id, count(*) AS ROWS
        FROM v_drug_exposure
        GROUP BY 1
        ORDER BY 2 desc
      )
  ) JOIN concept ON concept_id = drug_concept_id
);
-- Procedures
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'procedure_occurrence', 'Top 10 event concepts' as metric_name
     , 'concept_name' AS description_1, subString(concept_name, 1, 30) as value_1
     , 'rows' AS description_2, dtorok.f_formatNumber(rows)
FROM
(
  SELECT concept_name, rows
  FROM -- top 10 
  (
    SELECT top 10 *
    FROM -- concept_id with count
      (
        SELECT procedure_concept_id, count(*) AS ROWS
        FROM v_procedure_occurrence
        GROUP BY 1
        ORDER BY 2 desc
      )
  ) JOIN concept ON concept_id = procedure_concept_id
);
-- Measurement

INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'measurement', 'Top 10 event concepts' as metric_name
     , 'concept_name' AS description_1, subString(concept_name, 1, 30) as value_1
     , 'rows' AS description_2, dtorok.f_formatNumber(rows)
FROM
(
  SELECT concept_name, rows
  FROM -- top 10 
  (
    SELECT top 10 *
    FROM -- concept_id with count
      (
        SELECT measurement_concept_id, count(*) AS ROWS
        FROM v_measurement
        GROUP BY 1
        ORDER BY 2 desc
      )
  ) JOIN concept ON concept_id = measurement_concept_id
);

-- Observation
INSERT INTO dtorok.cdm_stats( log_date, dataSet_name, table_name, metric_name
                     , description_1, value_1, description_2, value_2)
SELECT getdate(), dtorok.dataSet_name(), 'observation', 'Top 10 event concepts' as metric_name
     , 'concept_name' AS description_1, subString(concept_name, 1, 30) as value_1
     , 'rows' AS description_2, dtorok.f_formatNumber(rows)
FROM
(
  SELECT concept_name, rows
  FROM -- top 10 
  (
    SELECT top 10 *
    FROM -- concept_id with count
      (
        SELECT observation_concept_id, count(*) AS ROWS
        FROM v_observation
        GROUP BY 1
        ORDER BY 2 desc
      )
  ) JOIN concept ON concept_id = observation_concept_id
);



