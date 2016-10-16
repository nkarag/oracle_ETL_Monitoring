CREATE OR REPLACE PACKAGE BODY DWMON_PACKAGE  AS
/******************************
Version 5.2
1. change in captured sessions (from V$session) to capture "Programs" with null (e.g. queries from DM_SPSS) 
    correct not to capture SYS, SYSTEM sessions etc. --> OK
2. change Insert with MERGE in the updating of vsession_snap --> OK
2.2 change merge of vsqlstats_snap: (Problem of previous version --> too slow query in the using() part of the merge)
     the new merge will get the rows found in v$sqlstats and insert/update the corresponding rows in vsqlstats_snap based on sql_id (no need for
     join with vsession_snap). 
     This will be imposed only to the rows of vsqlstats_snap that correspond to the maximum (latest) DB startup date (to ensure uniqueness of sql_ids
     i.e., ensure that only relevant sql_ids are updated)
     NOTE: for active sessions there might be query attrbutes that are not finalized (e.g. elapsed time of the query) untill the query finishes. 
     As this merge will be running every 1 minute or so the final values will be eventually recorded in the snap table.
     --> OK
3. include ETL statistics --> PENDING
    1. build new snap tables to record ETL info such as:
        a. ETL runtime errors (maybe)
        b. ETL flow executions (see: OWB manual Oracle® Warehouse Builder API and Scripting Reference, 1 Public Views for the Runtime Environment)
        c. ETL OWB errors (maybe)
        b. Data rejections (see rejection tables & shadow_dw.ETL_LOG)
        c. PDA job completion statistics
4. include BO statistics --> PENDING
5. include PDA report statistics --> PENDING


Version 5.1
1. change in captured sessions (from V$session) to capture "Programs" with null (e.g. queries from DM_SPSS) 
    correct not to capture SYS, SYSTEM sessions etc. --> OK
2. change Insert with MERGE in the updating of vsession_snap --> OK
2.2 change merge of vsqlstats_snap: (Problem of previous version --> too slow query in the using() part of the merge)
     the new merge will get the rows found in v$sqlstats and insert/update the corresponding rows in vsqlstats_snap based on sql_id (no need for
     join with vsession_snap). 
     This will be imposed only to the rows of vsqlstats_snap that correspond to the maximum (latest) DB startup date (to ensure uniqueness of sql_ids
     i.e., ensure that only relevant sql_ids are updated)
     NOTE: for active sessions there might be query attrbutes that are not finalized (e.g. elapsed time of the query) untill the query finishes. 
     As this merge will be running every 1 minute or so the final values will be eventually recorded in the snap table.
     --> OK
3. include ETL statistics --> PENDING
4. include BO statistics --> PENDING
5. include PDA report statistics --> PENDING


Version 5.0
1. change in captured sessions (from V$session) to capture "Programs" with null (e.g. queries from DM_SPSS) 
    correct not to capture SYS, SYSTEM sessions etc. --> OK
2. change Insert with MERGE in the updating of vsession_snap --> OK
3. include ETL statistics --> PENDING
4. include BO statistics --> PENDING
5. include PDA report statistics --> PENDING

Version 4.4
add all type of commands to be monitored not only SELECTS. Change in "dwmon_periodic_vsession_snap"

Version 4.3
add more sessions to  monitor (also from toad and sql navigator, to monitor target_dw, etl_dw etc. users)

Version 4.2
the rate_date in the DWH control table depicts the NEXT rate date, thus I corrected the logging procedure
to reflect this (subtracted 1 day from the rate_date

Version 4
New -->
    add a routine for logging the freshness of the data


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
             --s.command = 3 -- get only sessions doing a select.
             --and 
             s.status = 'ACTIVE' -- get only active sessions  
             and s.user# in (select user_id from mondw_users)
             and s.schemaname in (select username from mondw_users)
             and s.schemaname not in ('SYS', 'SYSTEM', 'SYSMAN', 'SYSADMIN')
             AND NOT REGEXP_LIKE (nvl(S.PROGRAM,'OK') , '(P\d\d\d)') -- dont get the rows corresponding to the parallel servers                                 
            /* and (s.program like '%ReportServer%' -- dont get the rows corresponding to the parallel servers
                 or s.program like '%busobj%'
                 or s.program like '%TOAD%'
                 or s.program like '%SQLNav%'
                 or s.program like '%sqlplus%')*/ 
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
    execute immediate 'alter session enable parallel dml';
    
    merge into MONITOR_DW.MONDW_VSESSION_SNAP sessionsnp 
    using (
    	   select  
               SADDR, SID, SERIAL#, 
               AUDSID, PADDR, USER#, 
               USERNAME, COMMAND, OWNERID, 
               TADDR, LOCKWAIT, STATUS, 
               SERVER, SCHEMA#, SCHEMANAME, 
               OSUSER, PROCESS, MACHINE, 
               TERMINAL, PROGRAM, TYPE, 
               SQL_ADDRESS, SQL_HASH_VALUE, SQL_ID, 
               SQL_CHILD_NUMBER, PREV_SQL_ADDR, PREV_HASH_VALUE, 
               PREV_SQL_ID, PREV_CHILD_NUMBER, PLSQL_ENTRY_OBJECT_ID, 
               PLSQL_ENTRY_SUBPROGRAM_ID, PLSQL_OBJECT_ID, PLSQL_SUBPROGRAM_ID, 
               MODULE, MODULE_HASH, ACTION, 
               ACTION_HASH, CLIENT_INFO, FIXED_TABLE_SEQUENCE, 
               ROW_WAIT_OBJ#, ROW_WAIT_FILE#, ROW_WAIT_BLOCK#, 
               ROW_WAIT_ROW#, LOGON_TIME, LAST_CALL_ET, 
               PDML_ENABLED, FAILOVER_TYPE, FAILOVER_METHOD, 
               FAILED_OVER, RESOURCE_CONSUMER_GROUP, PDML_STATUS, 
               PDDL_STATUS, PQ_STATUS, CURRENT_QUEUE_DURATION, 
               CLIENT_IDENTIFIER, BLOCKING_SESSION_STATUS, BLOCKING_INSTANCE, 
               BLOCKING_SESSION, SEQ#, EVENT#, 
               EVENT, P1TEXT, P1, 
               P1RAW, P2TEXT, P2, 
               P2RAW, P3TEXT, P3, 
               P3RAW, WAIT_CLASS_ID, WAIT_CLASS#, 
               WAIT_CLASS, WAIT_TIME, SECONDS_IN_WAIT, 
               STATE, SERVICE_NAME, SQL_TRACE, 
               SQL_TRACE_WAITS, SQL_TRACE_BINDS
               ,sysdate as POPULATION_DATE
               ,(select startup_time from v$instance) as INSTANCE_STARTUP_DATE
    	   from v$session s
    	   where 
                 --s.command = 3 -- get only sessions doing a select.
                 --and 
                 s.status = 'ACTIVE' -- get only active sessions  
                 and s.user# in (select user_id from mondw_users)
                 and s.schemaname in (select username from mondw_users)
                 and s.schemaname in (select username from mondw_users)
                 and s.schemaname not in ('SYS', 'SYSTEM', 'SYSMAN', 'SYSADMIN')
                 AND NOT REGEXP_LIKE (nvl(S.PROGRAM,'OK') , '(P\d\d\d)') -- dont get the rows corresponding to the parallel servers                                 
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
    				)           
    ) vsession
    ON (sessionsnp.SID = vsession.SID and sessionsnp.SQL_ID = vsession.SQL_ID and sessionsnp.LOGON_TIME = vsession.LOGON_TIME)
    WHEN MATCHED THEN
    UPDATE 
        SET    sessionsnp.SADDR                     = vsession.SADDR,
               --sessionsnp.SID                       = vsession.SID,
               sessionsnp.SERIAL#                   = vsession.SERIAL#,
               sessionsnp.AUDSID                    = vsession.AUDSID,
               sessionsnp.PADDR                     = vsession.PADDR,
               sessionsnp.USER#                     = vsession.USER#,
               sessionsnp.USERNAME                  = vsession.USERNAME,
               sessionsnp.COMMAND                   = vsession.COMMAND,
               sessionsnp.OWNERID                   = vsession.OWNERID,
               sessionsnp.TADDR                     = vsession.TADDR,
               sessionsnp.LOCKWAIT                  = vsession.LOCKWAIT,
               sessionsnp.STATUS                    = vsession.STATUS,
               sessionsnp.SERVER                    = vsession.SERVER,
               sessionsnp.SCHEMA#                   = vsession.SCHEMA#,
               sessionsnp.SCHEMANAME                = vsession.SCHEMANAME,
               sessionsnp.OSUSER                    = vsession.OSUSER,
               sessionsnp.PROCESS                   = vsession.PROCESS,
               sessionsnp.MACHINE                   = vsession.MACHINE,
               sessionsnp.TERMINAL                  = vsession.TERMINAL,
               sessionsnp.PROGRAM                   = vsession.PROGRAM,
               sessionsnp.TYPE                      = vsession.TYPE,
               sessionsnp.SQL_ADDRESS               = vsession.SQL_ADDRESS,
               sessionsnp.SQL_HASH_VALUE            = vsession.SQL_HASH_VALUE,
               --sessionsnp.SQL_ID                    = vsession.SQL_ID,
               sessionsnp.SQL_CHILD_NUMBER          = vsession.SQL_CHILD_NUMBER,
               sessionsnp.PREV_SQL_ADDR             = vsession.PREV_SQL_ADDR,
               sessionsnp.PREV_HASH_VALUE           = vsession.PREV_HASH_VALUE,
               sessionsnp.PREV_SQL_ID               = vsession.PREV_SQL_ID,
               sessionsnp.PREV_CHILD_NUMBER         = vsession.PREV_CHILD_NUMBER,
               sessionsnp.PLSQL_ENTRY_OBJECT_ID     = vsession.PLSQL_ENTRY_OBJECT_ID,
               sessionsnp.PLSQL_ENTRY_SUBPROGRAM_ID = vsession.PLSQL_ENTRY_SUBPROGRAM_ID,
               sessionsnp.PLSQL_OBJECT_ID           = vsession.PLSQL_OBJECT_ID,
               sessionsnp.PLSQL_SUBPROGRAM_ID       = vsession.PLSQL_SUBPROGRAM_ID,
               sessionsnp.MODULE                    = vsession.MODULE,
               sessionsnp.MODULE_HASH               = vsession.MODULE_HASH,
               sessionsnp.ACTION                    = vsession.ACTION,
               sessionsnp.ACTION_HASH               = vsession.ACTION_HASH,
               sessionsnp.CLIENT_INFO               = vsession.CLIENT_INFO,
               sessionsnp.FIXED_TABLE_SEQUENCE      = vsession.FIXED_TABLE_SEQUENCE,
               sessionsnp.ROW_WAIT_OBJ#             = vsession.ROW_WAIT_OBJ#,
               sessionsnp.ROW_WAIT_FILE#            = vsession.ROW_WAIT_FILE#,
               sessionsnp.ROW_WAIT_BLOCK#           = vsession.ROW_WAIT_BLOCK#,
               sessionsnp.ROW_WAIT_ROW#             = vsession.ROW_WAIT_ROW#,
               --sessionsnp.LOGON_TIME                = vsession.LOGON_TIME,
               sessionsnp.LAST_CALL_ET              = vsession.LAST_CALL_ET,
               sessionsnp.PDML_ENABLED              = vsession.PDML_ENABLED,
               sessionsnp.FAILOVER_TYPE             = vsession.FAILOVER_TYPE,
               sessionsnp.FAILOVER_METHOD           = vsession.FAILOVER_METHOD,
               sessionsnp.FAILED_OVER               = vsession.FAILED_OVER,
               sessionsnp.RESOURCE_CONSUMER_GROUP   = vsession.RESOURCE_CONSUMER_GROUP,
               sessionsnp.PDML_STATUS               = vsession.PDML_STATUS,
               sessionsnp.PDDL_STATUS               = vsession.PDDL_STATUS,
               sessionsnp.PQ_STATUS                 = vsession.PQ_STATUS,
               sessionsnp.CURRENT_QUEUE_DURATION    = vsession.CURRENT_QUEUE_DURATION,
               sessionsnp.CLIENT_IDENTIFIER         = vsession.CLIENT_IDENTIFIER,
               sessionsnp.BLOCKING_SESSION_STATUS   = vsession.BLOCKING_SESSION_STATUS,
               sessionsnp.BLOCKING_INSTANCE         = vsession.BLOCKING_INSTANCE,
               sessionsnp.BLOCKING_SESSION          = vsession.BLOCKING_SESSION,
               sessionsnp.SEQ#                      = vsession.SEQ#,
               sessionsnp.EVENT#                    = vsession.EVENT#,
               sessionsnp.EVENT                     = vsession.EVENT,
               sessionsnp.P1TEXT                    = vsession.P1TEXT,
               sessionsnp.P1                        = vsession.P1,
               sessionsnp.P1RAW                     = vsession.P1RAW,
               sessionsnp.P2TEXT                    = vsession.P2TEXT,
               sessionsnp.P2                        = vsession.P2,
               sessionsnp.P2RAW                     = vsession.P2RAW,
               sessionsnp.P3TEXT                    = vsession.P3TEXT,
               sessionsnp.P3                        = vsession.P3,
               sessionsnp.P3RAW                     = vsession.P3RAW,
               sessionsnp.WAIT_CLASS_ID             = vsession.WAIT_CLASS_ID,
               sessionsnp.WAIT_CLASS#               = vsession.WAIT_CLASS#,
               sessionsnp.WAIT_CLASS                = vsession.WAIT_CLASS,
               sessionsnp.WAIT_TIME                 = vsession.WAIT_TIME,
               sessionsnp.SECONDS_IN_WAIT           = vsession.SECONDS_IN_WAIT,
               sessionsnp.STATE                     = vsession.STATE,
               sessionsnp.SERVICE_NAME              = vsession.SERVICE_NAME,
               sessionsnp.SQL_TRACE                 = vsession.SQL_TRACE,
               sessionsnp.SQL_TRACE_WAITS           = vsession.SQL_TRACE_WAITS,
               sessionsnp.SQL_TRACE_BINDS           = vsession.SQL_TRACE_BINDS,
               sessionsnp.POPULATION_DATE           = vsession.POPULATION_DATE,
               sessionsnp.INSTANCE_STARTUP_DATE     = vsession.INSTANCE_STARTUP_DATE
    WHEN NOT MATCHED THEN INSERT (
           sessionsnp.SADDR, sessionsnp.SID, sessionsnp.SERIAL#, 
           sessionsnp.AUDSID, sessionsnp.PADDR, sessionsnp.USER#, 
           sessionsnp.USERNAME, sessionsnp.COMMAND, sessionsnp.OWNERID, 
           sessionsnp.TADDR, sessionsnp.LOCKWAIT, sessionsnp.STATUS, 
           sessionsnp.SERVER, sessionsnp.SCHEMA#, sessionsnp.SCHEMANAME, 
           sessionsnp.OSUSER, sessionsnp.PROCESS, sessionsnp.MACHINE, 
           sessionsnp.TERMINAL, sessionsnp.PROGRAM, sessionsnp.TYPE, 
           sessionsnp.SQL_ADDRESS, sessionsnp.SQL_HASH_VALUE, sessionsnp.SQL_ID, 
           sessionsnp.SQL_CHILD_NUMBER, sessionsnp.PREV_SQL_ADDR, sessionsnp.PREV_HASH_VALUE, 
           sessionsnp.PREV_SQL_ID, sessionsnp.PREV_CHILD_NUMBER, sessionsnp.PLSQL_ENTRY_OBJECT_ID, 
           sessionsnp.PLSQL_ENTRY_SUBPROGRAM_ID, sessionsnp.PLSQL_OBJECT_ID, sessionsnp.PLSQL_SUBPROGRAM_ID, 
           sessionsnp.MODULE, sessionsnp.MODULE_HASH, sessionsnp.ACTION, 
           sessionsnp.ACTION_HASH, sessionsnp.CLIENT_INFO, sessionsnp.FIXED_TABLE_SEQUENCE, 
           sessionsnp.ROW_WAIT_OBJ#, sessionsnp.ROW_WAIT_FILE#, sessionsnp.ROW_WAIT_BLOCK#, 
           sessionsnp.ROW_WAIT_ROW#, sessionsnp.LOGON_TIME, sessionsnp.LAST_CALL_ET, 
           sessionsnp.PDML_ENABLED, sessionsnp.FAILOVER_TYPE, sessionsnp.FAILOVER_METHOD, 
           sessionsnp.FAILED_OVER, sessionsnp.RESOURCE_CONSUMER_GROUP, sessionsnp.PDML_STATUS, 
           sessionsnp.PDDL_STATUS, sessionsnp.PQ_STATUS, sessionsnp.CURRENT_QUEUE_DURATION, 
           sessionsnp.CLIENT_IDENTIFIER, sessionsnp.BLOCKING_SESSION_STATUS, sessionsnp.BLOCKING_INSTANCE, 
           sessionsnp.BLOCKING_SESSION, sessionsnp.SEQ#, sessionsnp.EVENT#, 
           sessionsnp.EVENT, sessionsnp.P1TEXT, sessionsnp.P1, 
           sessionsnp.P1RAW, sessionsnp.P2TEXT, sessionsnp.P2, 
           sessionsnp.P2RAW, sessionsnp.P3TEXT, sessionsnp.P3, 
           sessionsnp.P3RAW, sessionsnp.WAIT_CLASS_ID, sessionsnp.WAIT_CLASS#, 
           sessionsnp.WAIT_CLASS, sessionsnp.WAIT_TIME, sessionsnp.SECONDS_IN_WAIT, 
           sessionsnp.STATE, sessionsnp.SERVICE_NAME, sessionsnp.SQL_TRACE, 
           sessionsnp.SQL_TRACE_WAITS, sessionsnp.SQL_TRACE_BINDS, sessionsnp.POPULATION_DATE, 
           sessionsnp.INSTANCE_STARTUP_DATE) 
    values (
        vsession.SADDR, vsession.SID, vsession.SERIAL#, 
            vsession.AUDSID, vsession.PADDR, vsession.USER#, vsession.USERNAME, vsession.COMMAND, vsession.OWNERID
            , vsession.TADDR
            , vsession.LOCKWAIT, vsession.STATUS, vsession.SERVER
            , vsession.SCHEMA#, vsession.SCHEMANAME, vsession.OSUSER
            , vsession.PROCESS, vsession.MACHINE, vsession.TERMINAL, vsession.PROGRAM, vsession.TYPE
            , vsession.SQL_ADDRESS, vsession.SQL_HASH_VALUE, vsession.SQL_ID
            , vsession.SQL_CHILD_NUMBER, vsession.PREV_SQL_ADDR, vsession.PREV_HASH_VALUE
            , vsession.PREV_SQL_ID, vsession.PREV_CHILD_NUMBER, vsession.PLSQL_ENTRY_OBJECT_ID
            , vsession.PLSQL_ENTRY_SUBPROGRAM_ID, vsession.PLSQL_OBJECT_ID, vsession.PLSQL_SUBPROGRAM_ID
            , vsession.MODULE, vsession.MODULE_HASH, vsession.ACTION
            , vsession.ACTION_HASH, vsession.CLIENT_INFO, vsession.FIXED_TABLE_SEQUENCE
            , vsession.ROW_WAIT_OBJ#, vsession.ROW_WAIT_FILE#, vsession.ROW_WAIT_BLOCK#
            , vsession.ROW_WAIT_ROW#, vsession.LOGON_TIME, vsession.LAST_CALL_ET
            , vsession.PDML_ENABLED, vsession.FAILOVER_TYPE, vsession.FAILOVER_METHOD
            , vsession.FAILED_OVER, vsession.RESOURCE_CONSUMER_GROUP, vsession.PDML_STATUS
            , vsession.PDDL_STATUS, vsession.PQ_STATUS, vsession.CURRENT_QUEUE_DURATION
            , vsession.CLIENT_IDENTIFIER, vsession.BLOCKING_SESSION_STATUS, vsession.BLOCKING_INSTANCE
            , vsession.BLOCKING_SESSION, vsession.SEQ#, vsession.EVENT#, vsession.EVENT, vsession.P1TEXT
            , vsession.P1, vsession.P1RAW, vsession.P2TEXT, vsession.P2, vsession.P2RAW, vsession.P3TEXT
            , vsession.P3, vsession.P3RAW, vsession.WAIT_CLASS_ID, vsession.WAIT_CLASS#, vsession.WAIT_CLASS
            , vsession.WAIT_TIME, vsession.SECONDS_IN_WAIT, vsession.STATE, vsession.SERVICE_NAME, vsession.SQL_TRACE
            , vsession.SQL_TRACE_WAITS, vsession.SQL_TRACE_BINDS  
           , vsession.POPULATION_DATE
           , vsession.INSTANCE_STARTUP_DATE    
    );
    commit;
end if;

END dwmon_periodic_vsession_snap;

/**
Routine to be called periodically. It captures a snapshot of v$sqlstats only for sql statements executed
by sessions in mondw_vsession_snap that are not active (in order to get the final elapsed time).

Change incorporated in version 5.1:
    1.   Change merge of vsqlstats_snap: (Problem of previous version --> too slow query in the using() part of the merge)
         the new merge will get the rows found in v$sqlstats and insert/update the corresponding rows in vsqlstats_snap based on sql_id (no need for
         join with vsession_snap). 
         This will be imposed only to the rows of vsqlstats_snap that correspond to the maximum (latest) DB startup date (to ensure uniqueness of sql_ids
         i.e., ensure that only relevant sql_ids are updated)
         NOTE: for active sessions there might be query attrbutes that are not finalized (e.g. elapsed time of the query) untill the query finishes. 
         As this merge will be running every 1 minute or so the final values will be eventually recorded in the snap table.
*/
PROCEDURE dwmon_periodic_vsqlstats_snap
IS
BEGIN

execute immediate 'alter session enable parallel dml';

merge into MONITOR_DW.MONDW_VSQLSTATS_SNAP sqlsnp
using (
    select s.*, sysdate as populdate, (select startup_time from v$instance) as dbuptime
    from v$sqlstats  s
) sqlstats
ON (sqlstats.SQL_ID = sqlsnp.SQL_ID and sqlstats.sql_text = sqlsnp.sql_text and sqlstats.dbuptime = sqlsnp.INSTANCE_STARTUP_DATE)
WHEN MATCHED THEN
    UPDATE 
SET    --sqlsnp.SQL_TEXT                  = sqlstats.SQL_TEXT,
       sqlsnp.SQL_FULLTEXT              = sqlstats.SQL_FULLTEXT,
       --sqlsnp.SQL_ID                    = sqlstats.SQL_ID,
       sqlsnp.LAST_ACTIVE_TIME          = sqlstats.LAST_ACTIVE_TIME,
       sqlsnp.LAST_ACTIVE_CHILD_ADDRESS = sqlstats.LAST_ACTIVE_CHILD_ADDRESS,
       sqlsnp.PLAN_HASH_VALUE           = sqlstats.PLAN_HASH_VALUE,
       sqlsnp.PARSE_CALLS               = sqlstats.PARSE_CALLS,
       sqlsnp.DISK_READS                = sqlstats.DISK_READS,
       sqlsnp.DIRECT_WRITES             = sqlstats.DIRECT_WRITES,
       sqlsnp.BUFFER_GETS               = sqlstats.BUFFER_GETS,
       sqlsnp.ROWS_PROCESSED            = sqlstats.ROWS_PROCESSED,
       sqlsnp.SERIALIZABLE_ABORTS       = sqlstats.SERIALIZABLE_ABORTS,
       sqlsnp.FETCHES                   = sqlstats.FETCHES,
       sqlsnp.EXECUTIONS                = sqlstats.EXECUTIONS,
       sqlsnp.END_OF_FETCH_COUNT        = sqlstats.END_OF_FETCH_COUNT,
       sqlsnp.LOADS                     = sqlstats.LOADS,
       sqlsnp.VERSION_COUNT             = sqlstats.VERSION_COUNT,
       sqlsnp.INVALIDATIONS             = sqlstats.INVALIDATIONS,
       sqlsnp.PX_SERVERS_EXECUTIONS     = sqlstats.PX_SERVERS_EXECUTIONS,
       sqlsnp.CPU_TIME                  = sqlstats.CPU_TIME,
       sqlsnp.ELAPSED_TIME              = sqlstats.ELAPSED_TIME,
       sqlsnp.AVG_HARD_PARSE_TIME       = sqlstats.AVG_HARD_PARSE_TIME,
       sqlsnp.APPLICATION_WAIT_TIME     = sqlstats.APPLICATION_WAIT_TIME,
       sqlsnp.CONCURRENCY_WAIT_TIME     = sqlstats.CONCURRENCY_WAIT_TIME,
       sqlsnp.CLUSTER_WAIT_TIME         = sqlstats.CLUSTER_WAIT_TIME,
       sqlsnp.USER_IO_WAIT_TIME         = sqlstats.USER_IO_WAIT_TIME,
       sqlsnp.PLSQL_EXEC_TIME           = sqlstats.PLSQL_EXEC_TIME,
       sqlsnp.JAVA_EXEC_TIME            = sqlstats.JAVA_EXEC_TIME,
       sqlsnp.SORTS                     = sqlstats.SORTS,
       sqlsnp.SHARABLE_MEM              = sqlstats.SHARABLE_MEM,
       sqlsnp.TOTAL_SHARABLE_MEM        = sqlstats.TOTAL_SHARABLE_MEM,
       sqlsnp.TYPECHECK_MEM             = sqlstats.TYPECHECK_MEM,
       sqlsnp.POPULATION_DATE           = sqlstats.populdate
       --sqlsnp.INSTANCE_STARTUP_DATE     = sqlstats.INSTANCE_STARTUP_DATE         
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
,sqlstats.populdate
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

/**
Routine to be called every morning in order to log the freshness of the data
*/
Procedure dwmon_log_freshness_of_data
is
begin

insert into mondw_freshness 
select sysdate as snapshot_datetime 
    ,(select RUN_DATE from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'OTE_DW_LAST_RUN') OTE_DW_LAST_RUN
    ,(select RUN_DATE from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'RATE_DATE') RATE_DATE
    , trunc(sysdate) - (select trunc(RUN_DATE) from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'OTE_DW_LAST_RUN') as other_facts_days_diff
    ,trunc(sysdate) - (select trunc(RUN_DATE)-1 from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'RATE_DATE') as cdrs_days_diff
from dual;
commit;

end dwmon_log_freshness_of_data;

END  DWMON_PACKAGE;
/             

