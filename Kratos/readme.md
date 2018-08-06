# Kratos example dataset

Example data.
Each patient demostrates one or more features of CDM. Some features do not require a patient to demostrate

# Problems
Title describes the problem. If it is demostrated on a person_id, it is listed in brackets after title
## Days of supply (order and dispensation) (1)
Patient 1 data comes from EHR system that provides pharmacy prescription data data. At the same time, there is dispensation data. This is a holy grail type of situation and allows styding medication adherence to prescription. It is now 2018. Assume prescription for 90 days with 1 refill specified. Data from early 2000 indicate a visit to a provider on Jan 1 where a drug is prescribed with one refill. The patient picks up the prescription on Jan 4th. Without any visit to any doctor in the next 5 months, the patient picks up at pharmacy a refill 1 of this medcation on April 15th. 

## Data comments
The sample data has 1 row for original prescription, 1 row for ETL derived prescription data and 2 dispensation rows when patient 1 picked up medication from pharmacy.  

http://forums.ohdsi.org/t/days-supply-yet-again/4741/15  



## Hysterectomy (2;3)

Patient 2 had a hysterectomy at the age of 52 when no EHR data exist. At age 65, there is a code for 'history of hysterectomy' documented in claims data. Patient 3 had hysterectomy at age 56 and direct procedure is recorded (not as history code).



# Improvements
 - allow smooth loading into Postgres
 - allow smooth loading into multiple platforms
 - make concept_id clickable/hoverable to reveal concept name


# Why this name

Power of example in explaining something is helpful. God of power and strenght is Kratos
