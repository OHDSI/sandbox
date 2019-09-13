# UnitMorph

This repo contains a set of code that helps OMOP sites to convert measurements into data-driven target units.

ThemisConcepts OHDSI network study provided (presented at AMIA 2019 as poster) a single concensus unit for each measurement (e.g., cm for height). UnitMorph builds upon that result.


# Update history

## Sep 12,2019
Initial draft and test on mimic OMOP data.


# Assumptions

See sql code for the where clause for some main assumptions.

# CDM issues

Note that we don’t have the `source_unit_concept_id` formally in the CDM - so we must use what is in unit_concept_id and override it.  (and rely that the source value unit will reflect the units prior being converted)
See prefix new_ in the query


# Links

- https://ucum.nlm.nih.gov/ucum-lhc/demo.html#conversion (pick conversion on the top)


