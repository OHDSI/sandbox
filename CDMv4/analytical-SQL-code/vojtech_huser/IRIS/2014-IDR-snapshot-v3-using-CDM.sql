--IDR Snapshot version 2 (against CDM v4 schema) 
--using Redshift flavor of SQL


--each measure starts with prefix comment measure
--below the code are numbers for two IMEDS datasets
--measures are described in two publications available at https://code.google.com/p/idrsnapshot/downloads/list
-- (the pptx file is best)

--excerpt
--Methods:
-- The IDR assessment methodology consists of computing a set of quantitative measures referred to as IDR Snapshot. Our pilot study included a set of 8 measures that were specifically designed to be intuitive to interpret and would facilitate continuous monitoring. For example, the measure D3 assessed the total number of patients with at least one diagnosis, one laboratory result and one prescription. The IDR Snapshot also includes very basic measures, such as the total number of events (G1) and the total number of patients (G2) within the repository. We used the database of the Biomedical Translational Research Information System (BTRIS)[2] as the key data source  evaluated in this study. It integrates data from the NIH Clinical Center EHR system (Allscripts Sunrise Clinical Manager) and numerous other systems across several NIH institutes (e.g., National Cancer Institute’s C3D clinical trials data management system). The IDR Snapshot is an open source project (available at http://code.google.com/p/idrsnapshot) that uses Structured Query Language (ANSI SQL:2008) and can be executed on all major database platforms.




--pick a dataset
--SET SEARCH_PATH TO ge_cdm4;
SET SEARCH_PATH TO ccae_cdm4;






--measure
--G1
;
select 
(select count(*) from person)
+(select count(*) from observation)
+(select count(*) from condition_occurrence)
+(select count(*) from drug_exposure)
+(select count(*) from visit_occurrence)
+(select count(*) from death)
+(select count(*) from procedure_occurrence)
;


--ge 6.7B
--ccae 20.3B








--measure
--G2

;
select count(*) from person;

--ge 33 281 949
--ccae 141 805 491










--measure
--d2
;
select count(*) from
(
select distinct person_id from observation
intersect
select distinct person_id from condition_occurrence 
);	

--ge 31 531 535
--ccae 6 505 919





--measure
--d3
select count(*) from
(
select distinct person_id from observation
intersect
select distinct person_id from condition_occurrence 
intersect
select distinct person_id from drug_exposure
);	

--ge 28 530 190 
--ccae 5 939 621





--measure
--L1
--preparatory code
;
select * from visit_occurrence v 
join person p on v.person_id= p.person_id;

select * from visit_occurrence v 
join person p on v.person_id= p.person_id
limit 10;

select visit_start_date - year_of_birth, * from visit_occurrence v 
join person p on v.person_id= p.person_id
limit 10;


select date_part('year',visit_start_date) - year_of_birth aprox_age, * from visit_occurrence v 
join person p on v.person_id= p.person_id
limit 10;


select date_part('year',visit_start_date) - year_of_birth approx_age, * from visit_occurrence v 
join person p on v.person_id= p.person_id
where date_part('year',visit_start_date) - year_of_birth < 18
limit 10;



--L1 final code
select count(*) from
(
select distinct p.person_id from visit_occurrence v 
join person p on v.person_id= p.person_id
where date_part('year',visit_start_date) - year_of_birth < 18
)
;

--ge 5.9M
--ccae 30.6M





--measure
--L2
;
select count(*) from
(
select distinct p.person_id from visit_occurrence v join person p on v.person_id= p.person_id where date_part('year',visit_start_date) - year_of_birth < 18
intersect
select distinct p.person_id from visit_occurrence v join person p on v.person_id= p.person_id where date_part('year',visit_start_date) - year_of_birth >=18
)
;
--ge 0.741M
--ccae 3.4M




