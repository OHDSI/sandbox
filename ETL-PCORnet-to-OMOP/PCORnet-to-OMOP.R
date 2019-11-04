#OMOP concept IDs
concept<-read_rds('concept.rds')

#Diagnosis-------------------------------------

#read in file
dx<- read.csv(file='REDACTED_CDM_DIAGNOSIS.csv', header=TRUE, sep=",")

#filter concepts for just ICD
concept_dx<- concept %>% filter(vocabulary_id == 'ICD10CM'| vocabulary_id == 'ICD9CM' )

#remove . to make mapping effective
concept_dx$concept_code2= as.character(gsub("\\.", "", concept_dx$concept_code))
dx$DX2 = as.character(gsub("\\.", "", dx$DX))

#Map source value to OMOP concept
dx2 = left_join(x=dx, y=concept_dx, by=c("DX2"="concept_code2"))

#Create OMOP version
condition<-data.frame(dx2$PATID, dx2$concept_id, dx2$ADMIT_DATE,dx2$ENCOUNTERID, dx2$DX)
names(condition) <- c("person_id", "condition_concept_id","condition_start_date","visit_occurrence_id", "condition_source_value" )
condition$condition_start_date<-parse_date_time(condition$condition_start_date, orders = c( "mdy", "dmy","ymd"))
condition$condition_start_date<-as.Date(condition$condition_start_date)

#Export
saveRDS(condition, file = "condition_occurrence.rds")
condition %>% write_csv('condition_occurrence.csv')
