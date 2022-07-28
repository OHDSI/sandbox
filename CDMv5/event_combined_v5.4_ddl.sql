--
-- Event Combined v5.4
--
-- Change log
-- Who	When		What
-- dtk		7Dec2021		Remove cost id and domain, use id for cost id and COALESCE(domain, default_domain) as event domain



create table event_combined 
(id	BIGINT	NOT NULL
,person_id	BIGINT	NOT NULL
,concept_id	BIGINT	NOT NULL
,type_concept_id	BIGINT	NULL
,start_datetime	TIMESTAMP	NULL
,end_datetime	TIMESTAMP	NULL
,visit_occurrence_id	BIGINT	NULL
,visit_detail_id	BIGINT	NULL
,provider_id	INTEGER	NULL
,source_value	VARCHAR(50)	NULL
,source_concept_id	INTEGER	NULL
,quantity	FLOAT	NULL
,unit_concept_id	INTEGER	NULL
,unit_source_value	VARCHAR(50)	NULL
,unit_source_concept_id	INTEGER	NULL
,value_as_number	FLOAT	NULL
,value_as_concept_id	INTEGER	NULL
,value_as_string	VARCHAR(60)	NULL
,specimen_source_id	VARCHAR(50)	NULL
,anatomic_site_concept_id	INTEGER	NULL
,anatomic_site_source_value	VARCHAR(50)	NULL
,disease_status_concept_id	INTEGER	NULL
,disease_status_source_value	VARCHAR(50)	NULL
,modifier_concept_id	INTEGER	NULL
,modifier_source_value	VARCHAR(50)	NULL
,verbatim_end_date	DATE	NULL
,stop_reason	VARCHAR(20)	NULL
,refills	INTEGER	NULL
,days_supply	INTEGER	NULL
,sig	VARCHAR(255)	NULL
,route_concept_id	INTEGER	NULL
,route_source_value	VARCHAR(50)	NULL
,lot_number	VARCHAR(50)	NULL
,unique_device_id	VARCHAR(50)	NULL
,production_id	VARCHAR(255)	NULL
,condition_status_concept_id	INTEGER	NULL
,condition_status_source_value	VARCHAR(50)	NULL
,operator_concept_id	INTEGER	NULL
,value_source_value	VARCHAR(50)	NULL
,range_low	FLOAT	NULL
,range_high	FLOAT	NULL
,qualifier_concept_id	INTEGER	NULL
,qualifier_source_value	VARCHAR(50)	NULL
,reference_field_concept_id	INTEGER	NULL
,reference_event_id	INTEGER	NULL
,event_time	VARCHAR(10)	NULL
,domain	varchar(20)	NOT NULL
,default_domain	varchar(20) NOT NULL	
,amount_allowed	float	NULL
,cost_type_concept_id	integer	NULL
,currency_concept_id	integer	NULL
,drg_concept_id	integer	NULL
,drg_source_value	varchar(3)	NULL
,paid_by_patient	float	NULL
,paid_by_payer	float	NULL
,paid_by_primary	float	NULL
,paid_dispensing_fee	float	NULL
,paid_ingredient_cost	float	NULL
,paid_patient_coinsurance	float	NULL
,paid_patient_copay	float	NULL
,paid_patient_deductible	float	NULL
,payer_plan_period_id	integer	NULL
,revenue_code_concept_id	integer	NULL
,revenue_code_source_value	varchar(50)	NULL
,total_charge	float	NULL
,total_cost	float	NULL
,total_paid	float	NULL
,visit_occurrence_source_value varchar(250)
, source_table varchar(30) NOT NULL
, source_table_PK bigint  NOT NULL
);
