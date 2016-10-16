/******************************

Version 3
New -->
    routine to update mondw_users table
    dwmon_mondw_users_update
    
Version 2
New --> 
    routine to capture V$SQLSTATS (dwmon_periodic_vsqlstats_snap)
    main routine to call the two statistics capture routines (dwmon_main_gather_usg_stats)

Last updated: 26/6/2008
Author: nkarag
*******************************/

CREATE OR REPLACE PACKAGE BODY DWMON_PACKAGE  AS


/**
main routine to call the two statistics capture routines
*/
procedure dwmon_main_gather_usg_stats
is
begin
    dwmon_periodic_vsession_snap;
    dwmon_periodic_vsqlstats_snap;
end dwmon_main_gather_usg_stats; 

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

/**
Routine to be called periodically. It captures a snapshot of v$sqlstats only for sql statements executed
by sessions in mondw_vsession_snap that are not active (in order to get the final elapsed time).

*/
PROCEDURE dwmon_periodic_vsqlstats_snap
IS
BEGIN

merge into MONITOR_DW.MONDW_VSQLSTATS_SNAP sqlsnp
using (
    select  s.*, sysdate, (select startup_time from v$instance) as dbuptime
    from v$sqlstats s 
        JOIN 
        MONITOR_DW.MONDW_VSESSION_SNAP sessionsnp 
        ON (s.SQL_ID = sessionsnp.SQL_ID)
    where
        sessionsnp.SID not in (select nvl(SID, (select sid from MONITOR_DW.MONDW_VSESSION_SNAP where rownum = 1) ) from v$session where STATUS = 'ACTIVE') -- not for sessions still running a query in order to get the final elapsed time        
) sqlstats
ON (sqlstats.SQL_ID = sqlsnp.SQL_ID and sqlstats.sql_text = sqlsnp.sql_text)
WHEN NOT MATCHED THEN INSERT (
sqlsnp.INSTANCE_STARTUP_DATE
,sqlsnp.POPULATION_DATE
,sqlsnp.TYPECHECK_MEM
,sqlsnp.TOTAL_SHARABLE_MEM
,sqlsnp.SHARABLE_MEM
,sqlsnp.SORTS
,sqlsnp.JAVA_EXEC_TIME
,sqlsnp.PLSQL_EXEC_TIME
,sqlsnp.USER_IO_WAIT_TIME
,sqlsnp.CLUSTER_WAIT_TIME
,sqlsnp.CONCURRENCY_WAIT_TIME
,sqlsnp.APPLICATION_WAIT_TIME
,sqlsnp.AVG_HARD_PARSE_TIME
,sqlsnp.ELAPSED_TIME
,sqlsnp.CPU_TIME
,sqlsnp.PX_SERVERS_EXECUTIONS
,sqlsnp.INVALIDATIONS
,sqlsnp.VERSION_COUNT
,sqlsnp.LOADS
,sqlsnp.END_OF_FETCH_COUNT
,sqlsnp.EXECUTIONS
,sqlsnp.FETCHES
,sqlsnp.SERIALIZABLE_ABORTS
,sqlsnp.ROWS_PROCESSED
,sqlsnp.BUFFER_GETS
,sqlsnp.DIRECT_WRITES
,sqlsnp.DISK_READS
,sqlsnp.PARSE_CALLS
,sqlsnp.PLAN_HASH_VALUE
,sqlsnp.LAST_ACTIVE_CHILD_ADDRESS
,sqlsnp.LAST_ACTIVE_TIME
,sqlsnp.SQL_ID
,sqlsnp.SQL_FULLTEXT
,sqlsnp.SQL_TEXT
)
VALUES (
sqlstats.dbuptime
,sysdate
,sqlstats.TYPECHECK_MEM
,sqlstats.TOTAL_SHARABLE_MEM
,sqlstats.SHARABLE_MEM
,sqlstats.SORTS
,sqlstats.JAVA_EXEC_TIME
,sqlstats.PLSQL_EXEC_TIME
,sqlstats.USER_IO_WAIT_TIME
,sqlstats.CLUSTER_WAIT_TIME
,sqlstats.CONCURRENCY_WAIT_TIME
,sqlstats.APPLICATION_WAIT_TIME
,sqlstats.AVG_HARD_PARSE_TIME
,sqlstats.ELAPSED_TIME
,sqlstats.CPU_TIME
,sqlstats.PX_SERVERS_EXECUTIONS
,sqlstats.INVALIDATIONS
,sqlstats.VERSION_COUNT
,sqlstats.LOADS
,sqlstats.END_OF_FETCH_COUNT
,sqlstats.EXECUTIONS
,sqlstats.FETCHES
,sqlstats.SERIALIZABLE_ABORTS
,sqlstats.ROWS_PROCESSED
,sqlstats.BUFFER_GETS
,sqlstats.DIRECT_WRITES
,sqlstats.DISK_READS
,sqlstats.PARSE_CALLS
,sqlstats.PLAN_HASH_VALUE
,sqlstats.LAST_ACTIVE_CHILD_ADDRESS
,sqlstats.LAST_ACTIVE_TIME
,sqlstats.SQL_ID
,sqlstats.SQL_FULLTEXT
,sqlstats.SQL_TEXT
);

commit;

END dwmon_periodic_vsqlstats_snap;

/**
Routine to be called periodically. It updates mondw_users table
*/
PROCEDURE dwmon_mondw_users_update
IS
BEGIN

merge into MONITOR_DW.MONDW_USERS mdw_users
using (
    select username, user_id, created, password 
    from dba_users
    where 
    trunc(created) > to_date('23/06/2008', 'dd/mm/yyyy')
    and (default_tablespace = 'USERS' or default_tablespace like 'TS_%')
) newusers
ON (newusers.user_id = mdw_users.user_id)
WHEN NOT MATCHED THEN INSERT (
mdw_users.username, mdw_users.user_id, mdw_users.created, mdw_users.password
)values (newusers.username, newusers.user_id, newusers.created, newusers.password);

commit;
end dwmon_mondw_users_update;

END  DWMON_PACKAGE;
/             