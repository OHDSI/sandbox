#insert your connection here
w <- connect(connectionDetails)


g1<-'
select
(select count(*) from person)
+(select count(*) from observation)
+(select count(*) from condition_occurrence)
+(select count(*) from drug_exposure)
+(select count(*) from visit_occurrence)
+(select count(*) from death)
+(select count(*) from procedure_occurrence)
'
g2<-'
select count(*) from person
'


dset<-'mslr'
dsets<-c('ge','ccae','mslr')
for (dset in dsets)
{
qs<-sprintf('SET SEARCH_PATH TO %s_cdm4',dset)
qs
dbSendUpdate(w,qs)
#qs<-'select count(*) from person'
(rg1<-dbGetQuery(w,g1))
(rg2<-dbGetQuery(w,g2))

qs2<-'select count(*) from procedure_cost'
proc<-dbGetQuery(w,qs2)
print(sprintf('%s: G1 %s',dset   ,format(rg1,big.mark = ",")))
print(sprintf('%s: G2 %s',dset   ,format(rg2,big.mark = ",")))
print(sprintf('%s: proc cost %s' ,dset,format(proc,big.mark = ",")))
}


#format(count,big.mark = ",")
dbDisconnect(w)


