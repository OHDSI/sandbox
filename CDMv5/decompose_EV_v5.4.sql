CREATE VIEW CONDITION_OCCURRENCE
( condition_occurrence_id
, person_id 
, condition_concept_id 
, condition_start_date
, condition_start_datetime
, condition_end_date
, condition_end_datetime 
, condition_type_concept_id 
, condition_status_concept_id
, stop_reason
, provider_id 
, visit_occurrence_id 
, visit_detail_id 
, condition_source_value 
, condition_source_concept_id
, condition_status_source_value
  )
  AS
SELECT
  id
, person_id
, concept_id 
, CAST( start_datetime AS date )
, start_datetime
, CAST( end_dateTime AS date )
, end_dateTime
, COALESCE( type_concept_id, 0)
, COALESCE( condition_status_concept_id, 0 )
, stop_reason
, provider_id
, visit_occurrence_id
, visit_detail_id
, source_value
, COALESCE( source_concept_id, 0 )
, condition_status_source_value
FROM event_combined
WHERE COALESCE( DOMAIN, DEFAULT_DOMAIN) = 'Condition';

--------------------------------------------------

CREATE VIEW DRUG_EXPOSURE  
(drug_exposure_id
, person_id
, drug_concept_id
, drug_exposure_start_date
, drug_exposure_start_datetime
, drug_exposure_end_date
, drug_exposure_end_datetime
, verbatim_end_date
, drug_type_concept_id
, stop_reason
, refills
, quantity
, days_supply
, sig
, route_concept_id
, lot_number
, provider_id
, visit_occurrence_id
, visit_detail_id
, drug_source_value
, drug_source_concept_id
, route_source_value
, dose_unit_source_value
)
 AS
 SELECT
  id
, person_id
, COALESCE( concept_id, 0 )
, CAST( start_dateTime AS date )
, start_datetime
, CAST( end_dateTime AS date ) 
, end_datetime
, verbatim_end_date
, COALESCE(type_concept_id, 0 )
, stop_reason
, refills
, COALESCE(quantity, 1.0) 
, days_supply
, sig
, COALESCE( route_concept_id, 0 )
, lot_number
, provider_id
, visit_occurrence_id
, visit_detail_id
, source_value
, COALESCE( source_concept_id, 0 )
, route_source_value 
, unit_source_value 
FROM EVENT_COMBINED
WHERE COALESCE( DOMAIN, DEFAULT_DOMAIN) = 'Drug';

----------------------------------------------------

CREATE VIEW PROCEDURE_OCCURRENCE  
( procedure_occurrence_id
,  person_id
, procedure_concept_id
, procedure_date
, procedure_datetime
, procedure_end_date
, procedure_end_datetime
, procedure_type_concept_id
, modifier_concept_id
, quantity
, provider_id
, visit_occurrence_id
, visit_detail_id
, procedure_source_value
, procedure_source_concept_id
, modifier_source_value 
)
AS
SELECT
  id 
, person_id
, COALESCE( concept_id, 0 )
, CAST( start_dateTime AS DATE )
, start_dateTime
, CAST( end_dateTime  AS DATE )
, end_datetime
, COALESCE( type_concept_id, 0 )
, COALESCE( modifier_concept_id, 0 )
, CAST( COALESCE( quantity, 1 ) as INTEGER )
, provider_id 
, visit_occurrence_id 
, visit_detail_id
, source_value 
, COALESCE( source_concept_id, 0 )
, modifier_source_value
FROM EVENT_COMBINED
WHERE COALESCE( DOMAIN, DEFAULT_DOMAIN) = 'Procedure';

-------------------------------

CREATE VIEW DEVICE_EXPOSURE  
( device_exposure_id 
,  person_id
, device_concept_id
, device_exposure_start_date
, device_exposure_start_datetime
, device_exposure_end_date
, device_exposure_end_datetime
, device_type_concept_id
, unique_device_id
, production_id 
, quantity
, provider_id
, visit_occurrence_id
, visit_detail_id
, device_source_value
, device_source_concept_id
, unit_concept_id
, unit_source_value 
, unit_source_concept_id 
)
 AS
 SELECT 
   id
,  person_id
,  COALESCE(concept_id, 0 )
,  CAST( start_dateTime AS DATE )
,  start_datetime
,  CAST( end_dateTime AS date )
,  end_datetime
,  COALESCE( type_concept_id, 0 )
,  unique_device_id
,  production_id
,  COALESCE( quantity, 1.0 )
,  provider_id
,  visit_occurrence_id
,  visit_detail_id
,  source_value
,  COALESCE( source_concept_id, 0 )
,  COALESCE( unit_concept_id, 0 )
,  unit_source_value
,  COALESCE( unit_source_concept_id, 0 )
FROM EVENT_COMBINED
WHERE COALESCE( DOMAIN, DEFAULT_DOMAIN) = 'Device';

-----------------------------------------------------

CREATE VIEW MEASUREMENT
( measurement_id
,  person_id
, measurement_concept_id
, measurement_date
, measurement_datetime
, measurement_time
, measurement_type_concept_id
, operator_concept_id
, value_as_number
, value_as_concept_id
, unit_concept_id
, range_low
, range_high
, provider_id
, visit_occurrence_id
, visit_detail_id 
, measurement_source_value 
, measurement_source_concept_id
, unit_source_value
, unit_source_concept_id
, value_source_value
, measurement_event_id 
, meas_event_field_concept_id
)
AS
SELECT
   id 
,  person_id
,  COALESCE( concept_id, 0 )
,  CAST( start_dateTime AS DATE )
,  start_datetime
,  event_time
,  COALESCE( type_concept_id, 0 )
,  COALESCE( operator_concept_id, 0 )
,  value_as_number
,  COALESCE( value_as_concept_id, 0 )
,  COALESCE( unit_concept_id, 0 )
,  range_low 
,  range_high 
,  provider_id 
,  visit_occurrence_id
,  visit_detail_id
,  source_value
,  COALESCE( source_concept_id, 0 )
,  unit_source_value
,  COALESCE( unit_source_concept_id, 0 )
,  value_source_value
,  reference_event_id
, reference_field_concept_id
FROM EVENT_COMBINED
WHERE COALESCE( DOMAIN, DEFAULT_DOMAIN) = 'Measurement';

----------------------------------------------------------

CREATE VIEW OBSERVATION  
( observation_id
,  person_id
,  observation_concept_id 
,  observation_date
,  observation_datetime
,  observation_type_concept_id
,  value_as_number
,  value_as_string 
,  value_as_concept_id 
,  qualifier_concept_id
,  unit_concept_id 
,  provider_id
,  visit_occurrence_id
,  visit_detail_id
,  observation_source_value
,  observation_source_concept_id 
,  unit_source_value 
,  qualifier_source_value 
,  value_source_value 
,  observation_event_id 
,  obs_event_field_concept_id
)
AS
SELECT
   id
,  person_id
,  COALESCE( concept_id, 0 )
,  CAST( start_dateTime AS DATE)
,  start_datetime
,  COALESCE( type_concept_id, 0 )
,  value_as_number
,  value_as_string
,  COALESCE( value_as_concept_id, 0 )
,  COALESCE( qualifier_concept_id, 0 )
,  COALESCE( unit_concept_id, 0 )
,  provider_id
,  visit_occurrence_id
,  visit_detail_id
,  source_value
,  COALESCE( source_concept_id, 0 )
,  unit_source_value
,  qualifier_source_value
,  value_source_value
,  reference_event_id
,  COALESCE( reference_field_concept_id, 0 )
FROM EVENT_COMBINED
WHERE COALESCE( DOMAIN, DEFAULT_DOMAIN) = 'Observation';

---------------------------------------------

CREATE VIEW SPECIMEN  
( specimen_id
,  person_id
,  specimen_concept_id
,  specimen_type_concept_id 
,  specimen_date
,  specimen_datetime
,  quantity 
,  unit_concept_id 
,  anatomic_site_concept_id 
,  disease_status_concept_id 
,  specimen_source_id
,  specimen_source_value
,  unit_source_value 
,  anatomic_site_source_value 
,  disease_status_source_value
)
AS
SELECT
   id
,  person_id 
,  COALESCE( concept_id, 0 )
,  COALESCE( type_concept_id, 0 )
,  CAST( start_dateTime AS date )
,  start_datetime
,  quantity
,  COALESCE( unit_concept_id, 0 )
,  COALESCE( anatomic_site_concept_id, 0 )
,  COALESCE( disease_status_concept_id, 0 )
,  specimen_source_id 
,  source_value
,  unit_source_value 
,  anatomic_site_source_value
,  disease_status_source_value
FROM EVENT_COMBINED
WHERE COALESCE( DOMAIN, DEFAULT_DOMAIN) = 'Specimen';

----------------------------------------------------------------------

CREATE VIEW COST  -- WHERE TOTAL COST > 0
(cost_id
, cost_event_id
, cost_domain_id
, cost_type_concept_id
, currency_concept_id
, total_charge
, total_cost
, total_paid
, paid_by_payer
, paid_by_patient
, paid_patient_copay
, paid_patient_coinsurance
, paid_patient_deductible
, paid_by_primary
, paid_ingredient_cos
, paid_dispensing_fee
, payer_plan_period_id
, amount_allowed
, revenue_code_concept_id
, revenue_code_source_value
, drg_concept_id
, drg_source_value
)
AS
SELECT
  id  -- same as event id
, id
, COALESCE( DOMAIN, DEFAULT_DOMAIN)
, COALESCE( cost_type_concept_id, 0 )
  -- US Dollar
, COALESCE( currency_concept_id, 44818668 )
, total_charge
, total_cost
, total_paid
, paid_by_payer
, paid_by_patient 
, paid_patient_copay
, paid_patient_coinsurance
, paid_patient_deductible
, paid_by_primary
, paid_ingredient_cost
, paid_dispensing_fee
, ppp.payer_plan_period_id  -- if this is calculated in view, then column can be removed from event combined
, amount_allowed
, COALESCE( revenue_code_concept_id, 0 )
, revenue_code_source_value
, COALESCE( drg_concept_id, 0 )
, drg_source_value
FROM EVENT_COMBINED ec
LEFT OUTER JOIN payer_plan_period ppp ON ppp.person_id = ec.person_id
                                                             AND CAST( start_dateTime AS date ) 
							     BETWEEN payer_plan_period_start_date AND payer_plan_period_end_date
WHERE total_cost > 0.0 ;



