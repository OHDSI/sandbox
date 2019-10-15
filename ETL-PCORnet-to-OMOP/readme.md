# Intro

PCORnet CDM contains many similar fields to OMOP CDM. It is feasible to convert the data. Note that ETL for reverse direction exists also.


# Approach

Due to small size, ETL was done in R with final tables written using DBI::dbWriteTable
