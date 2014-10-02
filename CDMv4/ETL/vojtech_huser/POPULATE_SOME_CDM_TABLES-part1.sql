--This is SQL CODE for MS SQL Server that imports limited data out of NIH Clinical Center BTRIS schema into
--CDM v4  (note that not all columns of CDM4 are created and populated with data)


--visit
select e.Subject_GUID as person_id,dateadd(year, datediff(year, 0, e.primary_Date_Time),0) visit_start_date  
into visit_occurrence
from btris.dbo.Observation_Measurable e
group by  
e.Subject_GUID,dateadd(year, datediff(year, 0, e.primary_Date_Time),0) 



--person

select e.UID person_id
,year(e.Date_of_birth) year_of_birth
into person
from btris.dbo.Subject e



--death
select e.UID person_id
, e.Date_of_death death_date
into death
from btris.dbo.Subject e
where e.Date_of_death is not null


