
#source('conn.R')
options(scipen = 999) #disable exponent scientific notation
library(Achilles)
library(DatabaseConnector)
library(dplyr)
library(magrittr)
library(OhdsiRTools)
library(OhdsiSharing)
library(readr)


#if this error, just re-run the command
#[Amazon](500310) Invalid operation: current transaction is aborted, commands ignored until end of transaction block;

#Tools - Global Options - Appearance - change font from 10 to 12
options(scipen = 999) #disable exponent scientific notation

#sessionInfo()
#session_info()

library(devtools)
library(Achilles)



#Schema names for the CMS DeSynPUF 1,000 person dataset
cdmDatabaseSchema <- "CMSDESynPUF1k"
cohortsDatabaseSchema <- "CMSDESynPUF1kresults"

#Schema names for the CMS DeSynPUF 2.3 million person dataset
cdmDatabaseSchema <- "CMSDESynPUF23m"
cohortsDatabaseSchema <- "CMSDESynPUF23mresults"



cdmDatabaseSchema <- "mimiciii100"
cohortsDatabaseSchema <- "mimiciii100results"

resultsDatabaseSchema<-cohortsDatabaseSchema

resultsDatabaseSchema<-cohortsDatabaseSchema
workDatabaseSchema<-cohortsDatabaseSchema
workFolder <-getwd()
outputFolder <-workFolder



#Connection string for the OMOP database on Redshift
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                server = "ohdsitutorialtest2-ohdsielas-redshiftclustermulti-1sizz9gq0e4uq.cc8ltappgfjt.us-east-1.redshift.amazonaws.com/mycdm",
                                                                user = "master",
                                                                password = "Password1"
                                                                ,schema = cdmDatabaseSchema
                                                                #,schema = resultsDatabaseSchema
)


library(DatabaseConnector)
conn<-connect(connectionDetails)
dbDisconnect(conn)


user <- Sys.info()["user"][[1]]
resultsDatabaseSchema<-paste0(user,'_',resultsDatabaseSchema,'03')
resultsDatabaseSchema


create_schema_sql <- renderSql("create schema if not exists @a;", a = resultsDatabaseSchema)$sql
conn <- connect(connectionDetails)
query_results <- executeSql(conn, create_schema_sql,reportOverallTime = FALSE,progressBar = FALSE)
disconnect(conn)





cdmDatabaseSchema
resultsDatabaseSchema
(a<-Sys.time())
aResult4<-achilles(connectionDetails,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   resultsDatabaseSchema= resultsDatabaseSchema,
                   numThreads = 4,
                   sourceName = cdmDatabaseSchema,
                   cdmVersion = "5.3.0",
                   runHeel = TRUE,
                   runCostAnalysis = FALSE,
                   conceptHierarchy = FALSE
                   ,logMultiThreadPerformance = TRUE
                   ,outputFolder = paste0(workFolder,'/03-m100')
)
Sys.time()-a

aResult4$analysisIds

fetchAchillesAnalysisResults(connectionDetails,resultsDatabaseSchema,1)
fetchAchillesAnalysisResults(connectionDetails,resultsDatabaseSchema,3)

lkup_analyses<-read.csv(system.file("csv","achilles","achilles_analysis_details.csv",package="Achilles"),as.is=T)

#a_results<-DatabaseConnector::dbGetQuery(conn,paste0('select * from ',resultsDatabaseSchema,'.achilles_results'))
#works too
a_results<-DatabaseConnector::dbReadTable(conn, paste0(resultsDatabaseSchema,".achilles_results"))
names(lkup_analyses)
lkup_analyses %<>% rename_all(tolower)
a_results<-a_results %>% left_join(lkup_analyses) %>% select(-cost,-distribution,-distributed_field)
View(a_results)

#, by=c('analysis_id'='ANALYSIS_ID')) %>% select(-COST,-DISTRIBUTION,-DISTRIBUTED_FIELD)



lkup_rules   <-read.csv(system.file("csv","heel","heel_rules_all.csv",package="Achilles"),as.is=T)
lkup_derived <-read.csv(system.file("csv","heel","heel_results_derived_details.csv",package="Achilles"),as.is=T)
a_results_derived<-DatabaseConnector::dbReadTable(conn, paste0(resultsDatabaseSchema,".achilles_results_derived"))
names(a_results)
names(lkup_derived)
a_results_derived %<>% left_join(lkup_derived)



aResult4
#3 is missing again

aResult5<-achilles(connectionDetails,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   resultsDatabaseSchema= resultsDatabaseSchema,
                   numThreads = 20,
                   sourceName = cdmDatabaseSchema,
                   cdmVersion = "5.3.0",
                   runHeel = TRUE,
                   runCostAnalysis = FALSE,
                   conceptHierarchy = FALSE
                   ,logMultiThreadPerformance = TRUE
                   ,outputFolder = workFolder
                   ,analysisIds = c(1,2,3)
)



#------------end setup

install_github('OHDSI/Achilles')
#install_github('OHDSI/OhdsiSharing')
library(Achilles)
packageVersion('Achilles')



lkup_rules   <-read.csv(system.file("csv","heel","heel_rules_all.csv",package="Achilles"),as.is=T)
lkup_derived <-read.csv(system.file("csv","heel","heel_results_derived_details.csv",package="Achilles"),as.is=T)
lkup_analyses<-read.csv(system.file("csv","achilles","achilles_analysis_details.csv",package="Achilles"),as.is=T)



a<-checkThemis(connectionDetails,cdmDatabaseSchema,resultsDatabaseSchema,outputFolder )

