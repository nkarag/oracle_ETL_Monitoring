/**
We need a table to store the DW users that we wish to monitor
*/
drop table mondw_users
/

create table mondw_users as 
   select u2.*, u1.password from dwadmin.target_ote_users u1 join all_users u2 on (u1.USER_NAME = u2.USERNAME)
/

alter table MONITOR_DW.MONDW_USERS 
add       (full_name varchar2(100)
            ,fname varchar2(100)
            ,lname varchar2(100)
            ,email varchar2(100))
/
         
update MONITOR_DW.MONDW_USERS u1
set (full_name,fname,lname,email) = (select FULL_NAME, FNAME, LNAME , EMAIL  
                                            from  MONITOR_DW.USERSTEMP u2 
                                            where u1.USERNAME = upper(u2.USERNAME))
/
commit
/     

alter table MONITOR_DW.MONDW_USERS 
add       (user_category varchar2(20))
/

update MONITOR_DW.MONDW_USERS u1
set user_category = 'цдоп'
where username in (
'XRVAS'
,'DIMPOUL'
,'MGASPARI'
,'EMANOLAKAK'
,'DEMCHR'
,'AELIEZER'
,'JPAPADAK' 
,'SCHATZIS'
,'IKOROMILA'
,'MPSYLLAKI' 
,'KMAVR'
,'DIMZAR'
,'GEOVAS'
,'PLAGGAS'
,'AFOURLARIS'
,'GSTAMOPOUL'
,'THKOUNTOUR'
--,'FKYRITSI' 
,'AVEROPOULOU'
,'SBAKADIMA'
,'ABRATI'
,'AMARIS'
,'VANDRIELOS'
,'SBOTHOU'
,'GMOURIKIS'
,'VSKLIKA'
)
/
commit
/
update MONITOR_DW.MONDW_USERS u1
set user_category = 'IT'
where username in (
'EFOTOPOULOS'
,'VZORBAS'
,'LSINOS'
,'NKARAG'
,'APEX_PUBLIC_USER'
,'BO_OP_PORTAL'
)
/
commit
/

delete from MONITOR_DW.MONDW_USERS
where username = 'PMARKOPOULO' 

update MONITOR_DW.MONDW_USERS u1
set user_category = 'цдееп'
where user_category IS null

select * from mondw_users

/
commit
/
                                            
alter table MONITOR_DW.MONDW_USERS 
add       (funct_group varchar2(100)
            ,logical_group varchar2(100)
            ,gen_directorate varchar2(100)
            ,directorate varchar2(100))
/


            

/**
We need a table to stroe periodic snapshot from v$session
*/

drop table mondw_vsession_snap
/

create table mondw_vsession_snap as 	   
	   select s.*, sysdate as population_date, (select startup_time from v$instance) as instance_startup_date 
	   from v$session s
	   where 1=0
/
   
/**
This table will store snapshots of v$sqlstats in a similar manner that vsession_snap does for v$session
*/
drop table mondw_vsqlstats_snap
/

create table mondw_vsqlstats_snap as
   select q.*, sysdate as population_date, (select startup_time from v$instance) as instance_startup_date 
	   from v$sqlstats q
	   where 1=0
/

