#!/bin/bash

# Description:
#   This job downloads Athena vocabularies zip file and imports vocabularies in BigQuey dataset
# 
# Requirements:
#  - Docker, JDK 1.8 installed on host
#  - UMLS account for CPT4 processing
# 
# Usage example:
# ./athena_import.sh {Link to Athena ZIP file}
#
# Example:
# ./athena_import.sh https://athena.ohdsi.org/api/v1/vocabularies/zip/02000a51-b913-4a6c-925c-1e4dd7661eeb
#

# Set parameters - Adjust these parameters as needed
GCP_PROJECT=<GCP Project Name>
GCP_BUCKET=<GCP Bucket Name>
CPT4_USERNAME=<UMLS login>
CPT4_PASSWORD=<UMLS password>

# Do not chage after this line without need
if [ $# -ne 1 ]; then
    echo $0: usage: ./athena_import.sh link_to_file
    exit 1
fi

# Get and unarchive zip file from ATHENA
mkdir tmp
cd tmp/
VOCABULARIES_LINK=$1
wget $VOCABULARIES_LINK -O tmp.zip
unzip tmp.zip

# Process CPT4 vocabulary
chmod a+x ./cpt.sh
./cpt.sh $CPT4_USERNAME $CPT4_PASSWORD

# Due to direct import is not possible to BigQuery, we have to load and export in PostgreSQL first. 
# BigQuery cannot parse the DATE format from ATHENA files.

# Create Postgres Docker instance to process vocabularies for BigQuery
sudo docker rm -f vocabs
echo "If you see error reported, ignore"
sudo docker create \
--name=vocabs \
--restart=always \
--shm-size="2g" \
-p 5433:5432 \
-e POSTGRES_USER=ohdsi \
-e POSTGRES_PASSWORD=ohdsi \
-e POSTGRES_DB=postgres \
postgres:12 && sudo docker start vocabs
sleep 30

# Create PG tables
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi < ../sql/create_vocab_tables_postgres.sql

# Load data in PG tables
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY CONCEPT FROM '$(pwd)/CONCEPT.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY CONCEPT_ANCESTOR FROM '$(pwd)/CONCEPT_ANCESTOR.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY CONCEPT_CLASS FROM '$(pwd)/CONCEPT_CLASS.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY CONCEPT_SYNONYM FROM '$(pwd)/CONCEPT_SYNONYM.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY CONCEPT_RELATIONSHIP FROM '$(pwd)/CONCEPT_RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY DOMAIN FROM '$(pwd)/DOMAIN.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY DRUG_STRENGTH FROM '$(pwd)/DRUG_STRENGTH.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY RELATIONSHIP FROM '$(pwd)/RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY VOCABULARY FROM '$(pwd)/VOCABULARY.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"

# Export to CSV file in BigQuery format
mkdir bq_export
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY concept TO '$(pwd)/bq_export/concept.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY concept_ancestor TO '$(pwd)/bq_export/concept_ancestor.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY concept_class TO '$(pwd)/bq_export/concept_class.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY concept_relationship TO '$(pwd)/bq_export/concept_relationship.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY concept_synonym TO '$(pwd)/bq_export/concept_synonym.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY domain TO '$(pwd)/bq_export/domain.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY drug_strength TO '$(pwd)/bq_export/drug_strength.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY relationship TO '$(pwd)/bq_export/relationship.csv' CSV;"
PGPASSWORD=ohdsi psql -h localhost -p 5433 -d postgres -U ohdsi -c "\COPY vocabulary TO '$(pwd)/bq_export/vocabulary.csv' CSV;"

# Get vocabs version
VOCABS_VERSION=(`PGPASSWORD=ohdsi psql -h localhost -p 5433 -t -U ohdsi -d postgres -c "select replace(replace(vocabulary_version, 'v5.0 ', ''), '-', '_') from vocabulary where vocabulary_id='None';"`)
echo "Vocabulary version: $VOCABS_VERSION"
GCP_BQ_DATASET=athena_vocabs_$VOCABS_VERSION

# Remove PG Docker container
sudo docker rm -f vocabs

# Upload files to GCP bucket
gsutil cp ./bq_export/*.csv gs://$GCP_BUCKET

# Create BQ dataset
bq --location=US mk \
--dataset \
$GCP_PROJECT:$GCP_BQ_DATASET

# Prepare SQL file
SQL_FILE=$GCP_BQ_DATASET.sql
cp ../sql/create_vocab_tables_bigquery.sql $SQL_FILE
sed -i "s/@dataset/$GCP_BQ_DATASET/g" $SQL_FILE

# Create tables
bq query --use_legacy_sql=false < $SQL_FILE

# Load vocabularies in BQ from Bucket
bq load  --source_format=CSV  $GCP_BQ_DATASET.concept gs://$GCP_BUCKET/concept.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.concept_ancestor gs://$GCP_BUCKET/concept_ancestor.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.concept_class gs://$GCP_BUCKET/concept_class.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.concept_relationship gs://$GCP_BUCKET/concept_relationship.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.concept_synonym gs://$GCP_BUCKET/concept_synonym.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.domain gs://$GCP_BUCKET/domain.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.drug_strength gs://$GCP_BUCKET/drug_strength.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.relationship gs://$GCP_BUCKET/relationship.csv
bq load  --source_format=CSV  $GCP_BQ_DATASET.vocabulary gs://$GCP_BUCKET/vocabulary.csv

# Remove temp folder
cd ../
rm -fr ./tmp