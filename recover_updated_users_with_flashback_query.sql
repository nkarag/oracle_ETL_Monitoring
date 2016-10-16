update  MONITOR_DW.MONDW_USERS
set DIRECTORATE = 'ÃÄÐ'
where DIRECTORATE is null and GEN_DIRECTORATE = 'ÃÄÐ'


select *
from   MONITOR_DW.MONDW_USERS
 where GEN_DIRECTORATE = 'ÃÄÐ' and 
 DIRECTORATE is null
 
update  MONITOR_DW.MONDW_USERS
set DIRECTORATE = null
where user_id in 
(
select user_id
from  MONITOR_DW.MONDW_USERS AS OF TIMESTAMP TO_TIMESTAMP ('23-Nov-10 16:10:10.123000', 'DD-Mon-RR HH24:MI:SS.FF')
 where GEN_DIRECTORATE = 'ÃÄÐ' and 
 DIRECTORATE is null )

commit

 
commit