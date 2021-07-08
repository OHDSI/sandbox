The job is designed to download OMOP vocabularies from ATHENA and load into new BigQuery dataset.

Requirements:
- GCP SDK installed and configured including account
- GCP bucket created
- GCP account should have at least "BigQuery Editor" role
- Installed JDK 1.8 (for CPT4 vocabullary processing)
- Installed and running Docker service

Instructions:
On Linux/Mac box

1. Clone code
2. Run `chmod a+x ./athena_import.sh` command to make file executable
3. Edit `athena_import.sh` file and provide GCP settings and UMLS credentials in the code:
```
GCP_PROJECT=<GCP Project Name>
GCP_BUCKET=<GCP Bucket Name>
CPT4_USERNAME=<UMLS Login>
CPT4_PASSWORD=<UMLS Password>
```
4. Execute the command passing link to generated vocabularies pack by ATHENA:
```
./athena_import.sh {Link to Athena ZIP file}
```
For example:
```
./athena_import.sh https://athena.ohdsi.org/api/v1/vocabularies/zip/02000a51-b913-4a6c-925c-1e4dd7661eef
```
