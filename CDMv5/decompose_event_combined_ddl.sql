/*
   Create CDM clinical event and cost view from event_combined
   
   Change log
   Who		When		What	
   d Torok		22Aug2019		created
*/

CREATE VIEW procedure_occurrence
(procedure_occurrence_id
,person_id
,procedure_concept_id
,procedure_date
,procedure_datetime
,procedure_source_value
,procedure_source_concept_id
,procedure_source_vocabulary_id
,procedure_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,quantity
,modifier_concept_id
,modifier_source_value
)
AS
SELECT
 event_id
,person_id
,event_concept_id
,event_start_date
,event_start_datetime
,event_source_value
,event_source_concept_id
,event_source_vocabulary_id
,event_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,CAST(quantity AS integer)
,qualifier_concept_id
,qualifier_source_value
FROM event_combined
WHERE COALESCE(domain, default_domain) = 'Procedure';

CREATE VIEW drug_exposure
(drug_exposure_id
,person_id
,drug_concept_id
,drug_exposure_start_date
,drug_exposure_end_date
,drug_exposure_start_datetime
,drug_exposure_end_datetime
,drug_source_value
,drug_source_concept_id
,drug_source_vocabulary_id
,drug_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,quantity
,stop_reason
,refills
,days_supply
,sig
,dose_unit_concept_id
,lot_number
,route_concept_id
,route_source_value
,dose_unit_source_value
,verbatim_end_date
)
AS
SELECT 
 event_id
,person_id
,event_concept_id
,event_start_date
,event_end_date
,event_start_datetime
,event_end_datetime
,event_source_value
,event_source_concept_id
,event_source_vocabulary_id
,event_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,quantity
,stop_reason
,refills
,days_supply
,sig
,dose_unit_concept_id
,lot_number
,route_concept_id
,route_source_value
,dose_unit_source_value
,verbatim_end_date
FROM event_combined
WHERE COALESCE(domain, default_domain) = 'Drug';

CREATE VIEW device_exposure
(device_exposure_id
,person_id
,device_concept_id
,device_exposure_start_date
,device_exposure_end_date
,device_exposure_start_datetime
,device_exposure_end_datetime
,device_source_value
,device_source_concept_id
,device_source_vocabulary_id
,device_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,quantity
,unique_device_id
)
AS
SELECT
 event_id
,person_id
,event_concept_id
,event_start_date
,event_end_date
,event_start_datetime
,event_end_datetime
,event_source_value
,event_source_concept_id
,event_source_vocabulary_id
,event_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,quantity
,unique_device_id
FROM event_combined
WHERE COALESCE(domain, default_domain) = 'Device';


CREATE VIEW condition_occurrence
(condition_occurrence_id
,person_id
,condition_concept_id
,condition_start_date
,condition_end_date
,condition_start_datetime
,condition_end_datetime
,condition_source_value
,condition_source_concept_id
,condition_source_vocabulary_id
,condition_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,stop_reason
,condition_status_concept_id
,condition_status_source_value
)
AS
SELECT
 event_id
,person_id
,event_concept_id
,event_start_date
,event_end_date
,event_start_datetime
,event_end_datetime
,event_source_value
,event_source_concept_id
,event_source_vocabulary_id
,event_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,stop_reason
,event_status_concept_id
,event_status_source_value
FROM event_combined
WHERE COALESCE(domain, default_domain) = 'Condition';

CREATE VIEW measurement
(measurement_id
,person_id
,measurement_concept_id
,measurement_date
,measurement_datetime
,measurement_source_value
,measurement_source_concept_id
,measurement_source_vocabulary_id
,measurement_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,value_as_number
,value_as_concept_id
,operator_concept_id
,unit_concept_id
,unit_source_value
,value_source_value
,range_low
,range_high
)
AS
SELECT 
 event_id
,person_id
,event_concept_id
,event_start_date
,event_start_datetime
,event_source_value
,event_source_concept_id
,event_source_vocabulary_id
,event_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,value_as_number
,value_as_concept_id
,operator_concept_id
,unit_concept_id
,unit_source_value
,value_as_string
,range_low
,range_high
FROM event_combined
WHERE COALESCE(domain, default_domain) = 'Measurement';

CREATE VIEW observation
(observation_id
,person_id
,observation_concept_id
,observation_date
,observation_datetime
,observation_source_value
,observation_source_concept_id
,observation_source_vocabulary_id
,observation_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,qualifier_concept_id
,qualifier_source_value
,value_as_number
,value_as_concept_id
,unit_concept_id
,unit_source_value
,value_as_string
)
AS
SELECT
 event_id
,person_id
,event_concept_id
,event_start_date
,event_start_datetime
,event_source_value
,event_source_concept_id
,event_source_vocabulary_id
,event_type_concept_id
,visit_occurrence_id
,visit_detail_id
,provider_id
,qualifier_concept_id
,qualifier_source_value
,value_as_number
,value_as_concept_id
,unit_concept_id
,unit_source_value
,value_as_string
FROM event_combined
WHERE COALESCE(domain, default_domain) = 'Observation';

CREATE VIEW specimen
(specimen_id
,person_id
,specimen_concept_id
,specimen_date
,specimen_dateTime
,specimen_source_value
,specimen_source_id
,specimen_type_concept_id
,quantity
,unit_concept_id
,unit_source_value
,disease_status_concept_id
,disease_status_source_value
,anatomic_site_concept_id
,anatomic_site_source_value
)
AS
SELECT
 event_id
,person_id
,event_concept_id
,event_start_date
,event_start_datetime
,event_source_value
,event_source_concept_id
,event_type_concept_id
,CAST(quantity AS integer)
,unit_concept_id
,unit_source_value
,event_status_concept_id
,event_status_source_value
,anatomic_site_concept_id
,anatomic_site_source_value
FROM event_combined
WHERE COALESCE(domain, default_domain) = 'Specimen';

CREATE VIEW cost
(cost_id
,cost_event_id
,cost_domain_id
,cost_type_concept_id
,currency_concept_id
,total_charge
,total_cost
,total_paid
,paid_by_payer
,paid_by_patient
,paid_patient_copay
,paid_patient_coinsurance
,paid_patient_deductible
,paid_by_primary
,paid_ingredient_cost
,paid_dispensing_fee
,payer_plan_period_id
,amount_allowed
,revenue_code_concept_id
,revenue_code_source_value
,drg_concept_id
,drg_source_value
)
AS
SELECT
 cost_id
,event_id
,COALESCE( domain, default_domain)
,cost_type_concept_id
,currency_concept_id
,total_charge
,total_cost
,total_paid
,paid_by_payer
,paid_by_patient
,paid_patient_copay
,paid_patient_coinsurance
,paid_patient_deductible
,paid_by_primary
,paid_ingredient_cost
,paid_dispensing_fee
,payer_plan_period_id
,amount_allowed
,revenue_code_concept_id
,revenue_code_source_value
,drg_concept_id
,drg_source_value
FROM event_combined;






