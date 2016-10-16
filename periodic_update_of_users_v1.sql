/**********************

Script to periodically merge external file with users to mondw_users
 
***********************/

-- create a temp table
create table MONITOR_DW.USERSTEMP4
as select * from MONITOR_DW.USERSTEMP3 where 1 = 0;

-- load temp table from xls, via TOAD wizard.

merge into MONITOR_DW.MONDW_USERS mdwusrs
using (
    select  *
    from MONITOR_DW.USERSTEMP4 
) usrs_delta
ON (mdwusrs.USERNAME = UPPER(usrs_delta.username) )
WHEN MATCHED THEN UPDATE SET 
     mdwusrs.FNAME = upper(usrs_delta.FNAME) 
     ,mdwusrs.LNAME = upper(usrs_delta.LNAME)
     ,mdwusrs.email = usrs_delta.email
     ,mdwusrs.FUNCT_GROUP = usrs_delta.FUNCTIONAL_GROUP  
     ,mdwusrs.LOGICAL_GROUP  = usrs_delta.LOGICAL_GROUP
     ,mdwusrs.GEN_DIRECTORATE   = usrs_delta.GEN_DIRECT 
     ,mdwusrs.DIRECTORATE = usrs_delta.DIRECT; 

commit

/*
For Periferies DM
*/
update mondw_users
set GEN_DIRECTORATE = 'GDP'
where username in (
select username
from mondw_users
where
 (USERNAME like 'TT%'  
 or
 USERNAME like 'PERIF_%'
 or
 USERNAME like 'DIAM%'
 or USERNAME like 'FKOU%'
 or USERNAME like 'ANKARA%')
 and GEN_DIRECTORATE is null
 and USERNAME <> 'PERIF_OWB' 
)  

commit
   
--------------------
drop table userstemp3

create table userstemp3
as select * from userstemp2
where 1 = 0

merge into MONITOR_DW.MONDW_USERS mdwusrs
using (
    select  *
    from MONITOR_DW.USERSTEMP3 
) usrs_delta
ON (mdwusrs.USERNAME = UPPER(usrs_delta.username) )
WHEN MATCHED THEN UPDATE SET 
     mdwusrs.FNAME = usrs_delta.FNAME 
     ,mdwusrs.LNAME = usrs_delta.LNAME
     ,mdwusrs.email = usrs_delta.email
     ,mdwusrs.FUNCT_GROUP = usrs_delta.FUNCTIONAL_GROUP  
     ,mdwusrs.LOGICAL_GROUP  = usrs_delta.LOGICAL_GROUP
     ,mdwusrs.GEN_DIRECTORATE   = usrs_delta.GEN_DIRECT 
     ,mdwusrs.DIRECTORATE = usrs_delta.DIRECT 

commit

---------------------------

merge into MONITOR_DW.MONDW_USERS mdw_users
using (
    select username, user_id, created, password 
    from dba_users
) newusers
ON (newusers.user_id = mdw_users.user_id)
WHEN NOT MATCHED THEN INSERT (
mdw_users.username, mdw_users.user_id, mdw_users.created, mdw_users.password
)values (newusers.username, newusers.user_id, newusers.created, newusers.password);

commit     

/*WHEN NOT MATCHED THEN 
INSERT (
   mdwusrs.USERNAME, mdwusrs.FNAME, mdwusrs.LNAME, 
   mdwusrs.EMAIL, mdwusrs.FUNCT_GROUP, mdwusrs.LOGICAL_GROUP, 
   mdwusrs.GEN_DIRECTORATE, mdwusrs.DIRECTORATE) 
VALUES ( 
   usrs_delta.USERNAME, usrs_delta.FNAME, usrs_delta.LNAME, 
   usrs_delta.EMAIL, usrs_delta.FUNCTIONAL_GROUP, usrs_delta.LOGICAL_GROUP, 
   usrs_delta.GEN_DIRECT, usrs_delta.DIRECT);
*/

select * from mondw_users order by created

commit
/

---------------------------- scratch area --------------------------------

select * from MONITOR_DW.MONDW_USERS

update MONITOR_DW.MONDW_USERS  
set user_category = 'IT'
, funct_group = 'BI-TEAM дсап'
, logical_group = 'BI-TEAM дсап'
, gen_directorate = 'цдтп'
, directorate = 'дсап'
where user_id in (138, 139, 140, 186,136)


select * from dba_users where username = 'TARGET_DW'

select *
from monitor_dw.mondw_users
where GEN_DIRECTORATE is null
order by username