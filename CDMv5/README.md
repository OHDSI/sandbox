sandbox
=======

event_combined_v5.3 --
Definition of a single table to hold all v5.3 events.  Intent is to create views against this table for the Condition Occurrence, Drug Exposure ...  The view determines what table a row belongs to by using the domain the source code is mapped to 'Domain' or if there is no mapping in the vocabulary then the 'Default Domain'

source_to_concept_map_5_with_maps_to_value.sql --
Create a table similar to V4 source to concept map which has 'Maps to' and 'Maps to Value' relationships.


