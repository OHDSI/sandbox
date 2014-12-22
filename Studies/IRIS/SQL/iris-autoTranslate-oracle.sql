/*********************************************************************************
# Copyright 2014 Observational Health Data Sciences and Informatics
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/


/************************

script to evaluate CDM repository

last revised: Dec 17 2014

author:  Vojtech Huser

description:



*************************/

  /*useInterimTables:  1*/
  /*resultsSchema:  results*/
 /*studyName:  iris*/
 /*sourceName:  CCAE*/
 /*sourceName:  CCAE*/



--switch to the schema where tables can be created
ALTER SESSION SET current_schema =  results;

--For Oracle: drop temp tables if they already exist

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE  iris_A';
  EXECUTE IMMEDIATE 'DROP TABLE  iris_A';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;



--start of analysis


create table iris_A
(
	measure varchar(20) not null,
        result NUMBER(19),
        explanation varchar(255)
);


INSERT INTO iris_A (measure, result, explanation)
select  '02G2',a.cnt, 'count of patients'
FROM
(
	select count(*) cnt from ccae_cdm4.person
) a
;





INSERT INTO iris_A (measure, result, explanation)
select  '01G1',a.cnt, 'count of events'
FROM
(
select 
(select count(*)   from ccae_cdm4.person)
+(select count(*)  from ccae_cdm4.observation)
+(select count(*)  from ccae_cdm4.condition_occurrence)
+(select count(*)  from ccae_cdm4.drug_exposure)
+(select count(*)  from ccae_cdm4.visit_occurrence)
+(select count(*)  from ccae_cdm4.death)
+(select count(*)  from ccae_cdm4.procedure_occurrence) cnt
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D2',a.cnt, 'count of patients with at least 1 Dx and 1 Rx'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from ccae_cdm4.condition_occurrence
  intersect
  select distinct person_id from ccae_cdm4.drug_exposure
  ) b
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D3',a.cnt, 'count of patients with at least 1 Dx and 1 Proc'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from ccae_cdm4.condition_occurrence
  intersect
  select distinct person_id from ccae_cdm4.procedure_occurrence
  ) b
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D4',a.cnt, 'count of patients with at least 1 Obs, 1 Dx and 1 Rx'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from ccae_cdm4.observation
  intersect
  select distinct person_id from ccae_cdm4.condition_occurrence
  intersect
  select distinct person_id from ccae_cdm4.drug_exposure
  ) b
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D5',a.cnt, 'count of deceased patients'
FROM
(
  select count(*) cnt from ccae_cdm4.death  
) a
;



--use this last command  to extract the data (uncomment it first)
--select * from iris_A;

