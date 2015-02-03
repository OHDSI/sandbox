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

{DEFAULT @useInterimTables = 1}  /*useInterimTables:  @useInterimTables*/
{DEFAULT @resultsSchema = 'irisResultsSchema'}  /*resultsSchema:  @resultsSchema*/
{DEFAULT @studyName = 'iris'} /*studyName:  @studyName*/
{DEFAULT @sourceName = 'source'} /*sourceName:  @sourceName*/
{DEFAULT @cdmsourceName = 'source'} /*sourceName:  @sourceName*/



--switch to the schema where tables can be created
{@useInterimTables ==1} ? {USE @resultsSchema;} : {--select 'not using interim tables'}

--For Oracle: drop temp tables if they already exist
{@useInterimTables ==1} ? {
IF OBJECT_ID('@studyName_A', 'U') IS NOT NULL
  DROP TABLE @studyName_A;
}


--start of analysis

{@useInterimTables ==1} ? {
create table @studyName_A
(
	measure varchar(20) not null,
        result bigint,
        explanation varchar(255)
);
}

{@useInterimTables ==1} ? {INSERT INTO @studyName_A (measure, result, explanation)}
select  '02G2',a.cnt, 'count of patients'
FROM
(
	select count(*) cnt from @cdmSchema.person
) a
;





{@useInterimTables ==1} ? {INSERT INTO @studyName_A (measure, result, explanation)}
select  '01G1',a.cnt, 'count of events'
FROM
(
select 
(select count(*)   from @cdmSchema.person)
+(select count(*)  from @cdmSchema.observation)
+(select count(*)  from @cdmSchema.condition_occurrence)
+(select count(*)  from @cdmSchema.drug_exposure)
+(select count(*)  from @cdmSchema.visit_occurrence)
+(select count(*)  from @cdmSchema.death)
+(select count(*)  from @cdmSchema.procedure_occurrence) cnt
) a
;


{@useInterimTables ==1} ? {INSERT INTO @studyName_A (measure, result, explanation)}
select  'D2',a.cnt, 'count of patients with at least 1 Dx and 1 Rx'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from @cdmSchema.condition_occurrence
  intersect
  select distinct person_id from @cdmSchema.drug_exposure
  ) b
) a
;


{@useInterimTables ==1} ? {INSERT INTO @studyName_A (measure, result, explanation)}
select  'D3',a.cnt, 'count of patients with at least 1 Dx and 1 Proc'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from @cdmSchema.condition_occurrence
  intersect
  select distinct person_id from @cdmSchema.procedure_occurrence
  ) b
) a
;


{@useInterimTables ==1} ? {INSERT INTO @studyName_A (measure, result, explanation)}
select  'D4',a.cnt, 'count of patients with at least 1 Obs, 1 Dx and 1 Rx'
FROM
(
  select count(*) cnt from
  (
  select distinct person_id from @cdmSchema.observation
  intersect
  select distinct person_id from @cdmSchema.condition_occurrence
  intersect
  select distinct person_id from @cdmSchema.drug_exposure
  ) b
) a
;


{@useInterimTables ==1} ? {INSERT INTO @studyName_A (measure, result, explanation)}
select  'D5',a.cnt, 'count of deceased patients'
FROM
(
  select count(*) cnt from @cdmSchema.death  
) a
;



--use this last command  to extract the data (uncomment it first)
--{@useInterimTables ==1} ? {select * from @studyName_A;}

