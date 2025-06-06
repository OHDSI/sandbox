# UnitMorph

This repo contains a set of code that helps OMOP sites to convert measurements into data-driven target units.

ThemisConcepts OHDSI network study provided (presented at AMIA 2019 as poster) a single concensus unit for each measurement (e.g., cm for height; see https://github.com/vojtechhuser/ThemisConcepts/blob/master/extras/results2019/S7-preferred_units-ABC.csv). UnitMorph builds upon that result.


# Update history

## Sep 12,2019
Initial draft and test on mimic OMOP data. Current SQL is not an `UPDATE` statement but a view of the data after join with the `CONV` table. It is a view that shows new values. The local ETL developer may easy turn the view into UPDATE statement (in the dialect of his/her DBMS).


# Assumptions

See sql code for the where clause for some main assumptions.

# CDM issues

Note that we don’t have the `source_unit_concept_id` formally in the CDM - so we must use what is in unit_concept_id and override it.  (and rely that the source value unit will reflect the units prior being converted)
See prefix new_ in the query

# implementation
compared with 2019 - no change to CDM is required

## A
no change to DQD, no change to vocabulary and unitmorg.csv and SQL is provided in seperate repo. Add to their ETL (or not) based on their choice.
follows OMOP principle of full preservation of original data (both data is in CDM preserved)

## B
+ DQD (kb for conversion is more integrated)

## C
prefered unit knowledge is relationship in vocabulary
con: any update takes half a year
pro: 


# Links

- https://ucum.nlm.nih.gov/ucum-lhc/demo.html#conversion (pick conversion on the top)


