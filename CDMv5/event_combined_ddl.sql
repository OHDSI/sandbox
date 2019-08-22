CREATE table event_combined
(event_id	INTEGER	NOT NULL
,person_id	INTEGER	NOT NULL
,event_concept_id	INTEGER	NOT NULL
,event_start_date	DATE	NOT NULL
,event_end_date	DATE	NULL
,event_start_datetime	DATETIME	NULL
,event_end_datetime	DATETIME	NULL
,event_source_value	VARCHAR(50)	NULL 
,event_source_concept_id	INTEGER	NULL  
,event_source_vocabulary_id	varchar(20)	NULL  
,event_type_concept_id	INTEGER	NOT NULL
,visit_occurrence_id	INTEGER	NULL  
,visit_detail_id	INTEGER	NULL  
,provider_id	INTEGER	NULL  
,quantity	FLOAT	NULL
,stop_reason	VARCHAR(20)	NULL  
,qualifier_concept_id	INTEGER	NOT NULL
,unique_device_id	VARCHAR(50)	NULL 
,qualifier_source_value	VARCHAR(50)	NULL
,refills	INTEGER	NULL
,days_supply	INTEGER	NULL
,sig	varchar(2000)	NULL
,dose_unit_concept_id	INTEGER	NOT NULL
,lot_number	VARCHAR(50)	NULL
,route_concept_id	INTEGER	NOT NULL
,route_source_value	VARCHAR(50)	NULL
,dose_unit_source_value	VARCHAR(50)	NULL
,value_as_number	FLOAT	NULL  
,value_as_concept_id	INTEGER	NOT NULL
,operator_concept_id	INTEGER	NOT NULL
,unit_concept_id	INTEGER	NOT NULL
,unit_source_value	VARCHAR(50)	NULL 
,value_as_string	VARCHAR(50)	NULL
,range_low	FLOAT	NULL  
,range_high	FLOAT	NULL  
,domain	VARCHAR(20)	NULL
,default_domain	varchar(20)	NOT NULL
,event_status_concept_id	INTEGER	NULL  
,event_status_source_value	VARCHAR(50)	NULL  
,anatomic_site_concept_id	INTEGER	NULL
,anatomic_site_source_value	VARCHAR(50)	NULL
,verbatim_end_date	date	NULL
,cost_id	INTEGER	NOT NULL
,cost_event_id	INTEGER	NOT NULL
,cost_domain_id	VARCHAR(20)	NOT NULL
,cost_type_concept_id	INTEGER	NOT NULL
,currency_concept_id	INTEGER	NOT NULL
,total_charge	NUMERIC(10,2)	
,total_cost	NUMERIC(10,2)	
,total_paid	NUMERIC(10,2)	
,paid_by_payer	NUMERIC(10,2)	
,paid_by_patient	NUMERIC(10,2)	
,paid_patient_copay	NUMERIC(10,2)	
,paid_patient_coinsurance	NUMERIC(10,2)	
,paid_patient_deductible	NUMERIC(10,2)	
,paid_by_primary	NUMERIC(10,2)	
,paid_ingredient_cost	NUMERIC(10,2)	
,paid_dispensing_fee	NUMERIC(10,2)	
,payer_plan_period_id	INTEGER	NOT NULL
,amount_allowed	NUMERIC(10,2)	
,revenue_code_concept_id	INTEGER	NOT NULL
,revenue_code_source_value	VARCHAR(4)	
,drg_concept_id	INTEGER	NOT NULL
,drg_source_value	VARCHAR(3)	
);