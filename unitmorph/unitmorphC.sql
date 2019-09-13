select 
 m.measurement_concept_id
 ,m.value_as_number * c.factor as new_value_as_number
 ,c.target_unit_concept_id as new_unit_concept_id
 ,m.value_as_number
 ,m.unit_concept_id
from measurement m 
 join conv c on
  m.measurement_concept_id=c.measurement_concept_id 
  and m.unit_concept_id=c.unit_concept_id
 where m.value_as_number is not null  
