CREATE OR REPLACE PACKAGE BODY DWMON_PACKAGE  AS

/**
Code to fill the vsession_snap table. This must be called periodically e.g. every 3 minutes or so.
We retrieve from v$session only the rows of interest (i.e., sessions executing a select and belonging to a DWH user)
that have changed from the last snap. This is achieved through the v$session.fixed_table_sequence column. 

Also,
Code to initialize vsession_snap: this must be called the first time we want to fill vsession_snap 
after each database startup. During initialization we also include the v$session row corresponding to
the insert statement of MONITOR_DW just to ensure that at least one row will be inserted into vsession_snap
during initialization. Therefore these records in vsession_snap signify an initialization of the table

This is necessary because the statement that periodically fills vsession_snap, does
a select on the vsession_snap based on the db instance startup date and we need at least one row to be returned from
this select. 
*/
PROCEDURE dwmon_periodic_vsession_snap
IS
last_startup_date mondw_vsession_snap.INSTANCE_STARTUP_DATE%TYPE;
curr_startup_date v$instance.STARTUP_TIME%TYPE;
BEGIN

select nvl(max(INSTANCE_STARTUP_DATE), to_date('01/01/1900','dd/mm/yyyy')) into last_startup_date 
from mondw_vsession_snap;

--dbms_output.PUT_LINE(last_startup_date);

select startup_time into curr_startup_date from v$instance;

--dbms_output.PUT_LINE(curr_startup_date);

if ( curr_startup_date <> last_startup_date) then -- a startup has occured since the previous snapshot, we must init the table
--debug
--dbms_output.PUT_LINE('must init');
--end debug
    insert into mondw_vsession_snap
	   select s.*, sysdate, (select startup_time from V$INSTANCE)
	   from v$session s
	   where
	   		( 
             s.command = 3 -- get only sessions doing a select.
             and s.status = 'ACTIVE' -- get only active sessions  
             and s.user# in (select user_id from mondw_users)
             and s.schemaname in (select username from mondw_users)
             and (s.program like '%ReportServer%' -- dont get the rows corresponding to the parallel servers
                 or s.program like '%busobj%') 
			 )
			 or
             (
			 s.username = 'MONITOR_DW' --include also MONITOR_DW in case no user is connected, just to ensure that at least one row will be inserted for the initialization of vsession_snap
             and s.command = 2 -- this is the current insert statement
             and s.status = 'ACTIVE'
             );
    commit;
else
--debug
--dbms_output.PUT_LINE('must NOT init');
--end debug
    insert into mondw_vsession_snap
    	   select s.*, sysdate, (select startup_time from v$instance)
    	   from v$session s
    	   where 
                 s.command = 3 -- get only sessions doing a select.
                 and s.status = 'ACTIVE' -- get only active sessions  
                 and s.user# in (select user_id from mondw_users)
                 and s.schemaname in (select username from mondw_users)
                 and (s.program like '%ReportServer%' -- dont get the rows corresponding to the parallel servers
                     or s.program like '%busobj%') 
    			 and s.FIXED_TABLE_SEQUENCE > 
    			 	(
                      select FIXED_TABLE_SEQUENCE --get the fixed_table_sequence of the most recent popoulation_date within the current DB startup time
                      from (
                      select ss.*
                      	   ,row_number() over (order by population_date  desc, FIXED_TABLE_SEQUENCE desc) as R
                      from mondw_vsession_snap ss
                      where 
                      ss.instance_startup_date =  (select startup_time from v$instance) -- fixed_table_sequence will not be consistent between different database statups
                      ) t
                      where t.R = 1 -- get only the most recent population date			 
    				);   
    commit;
end if;

END dwmon_periodic_vsession_snap;

END  DWMON_PACKAGE;
/             