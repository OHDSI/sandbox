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
USE results;

--For Oracle: drop temp tables if they already exist

IF OBJECT_ID('iris_A', 'U') IS NOT NULL
  DROP TABLE iris_A;



--start of analysis


create table iris_A
(
	measure varchar(20) not null,
        result bigint,
        explanation varchar(255)
);


INSERT INTO iris_A (measure, result, explanation)
select  '02G2',a.cnt, 'count of patients'
FROM
(
	select count(*) cnt from ccae_cdm4.dbo.person
) a
;





INSERT INTO iris_A (measure, result, explanation)
select  '01G1',a.cnt, 'count of events'
FROM
(
select 
(select count(*)   from ccae_cdm4.dbo.person)
+(select count(*)  from ccae_cdm4.dbo.observation)
+(select count(*)  from ccae_cdm4.dbo.condition_occurrence)
+(select count(*)  from ccae_cdm4.dbo.drug_exposure)
+(select count(*)  from ccae_cdm4.dbo.visit_occurrence)
+(select count(*)  from ccae_cdm4.dbo.death)
+(select count(*)  from ccae_cdm4.dbo.procedure_occurrence) cnt
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D2',a.cnt, 'count of patients with at least 1 Dx and 1 Rx'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from ccae_cdm4.dbo.condition_occurrence
  intersect
  select distinct person_id from ccae_cdm4.dbo.drug_exposure
  ) b
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D3',a.cnt, 'count of patients with at least 1 Dx and 1 Proc'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from ccae_cdm4.dbo.condition_occurrence
  intersect
  select distinct person_id from ccae_cdm4.dbo.procedure_occurrence
  ) b
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D4',a.cnt, 'count of patients with at least 1 Obs, 1 Dx and 1 Rx'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from ccae_cdm4.dbo.observation
  intersect
  select distinct person_id from ccae_cdm4.dbo.condition_occurrence
  intersect
  select distinct person_id from ccae_cdm4.dbo.drug_exposure
  ) b
) a
;


INSERT INTO iris_A (measure, result, explanation)
select  'D5',a.cnt, 'count of deceased patients'
FROM
(
  select count(*) cnt from ccae_cdm4.dbo.death  
) a
;



--use this last command  to extract the data (uncomment it first)
--select * from iris_A;

