
-- elapsed time 04 mins:16 sec
SELECT s.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime, count(*) over()
FROM v$sqlstats s JOIN monitor_dw.mondw_vsession_snap sessionsnp
      ON (s.sql_id = sessionsnp.sql_id)            
WHERE sessionsnp.SID NOT IN (
            SELECT NVL (SID,
                        (SELECT SID
                           FROM monitor_dw.mondw_vsession_snap
                          WHERE ROWNUM = 1))
              FROM v$session
             WHERE status = 'ACTIVE')

-- elapsed time 04 mins:25 sec             
SELECT s.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime
FROM v$sqlstats s JOIN monitor_dw.mondw_vsession_snap sessionsnp
      ON (s.sql_id = sessionsnp.sql_id)
      join (select sid from v$session where status <> 'ACTIVE') t
      on(sessionsnp.sid = t.sid)

-- elapsed 469 msecs
with q1 as
(
select * from v$sqlstats
),
q2 as
(
select sid
from v$session where status <> 'ACTIVE'
)
select q1.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime, count(*) over()
from
(q1 JOIN monitor_dw.mondw_vsession_snap sessionsnp
      ON (q1.sql_id = sessionsnp.sql_id)
)      
join q2 on(sessionsnp.sid = q2.sid)                                           


-- elapsed 469 msecs
with q1 as
(
select * from v$sqlstats
),
q2 as
(
select sid
from v$session where status <> 'ACTIVE'
)
select q1.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime, count(*) over()
from
(q1 JOIN monitor_dw.mondw_vsession_snap sessionsnp
      ON (q1.sql_id = sessionsnp.sql_id)
)      
join q2 on(sessionsnp.sid = q2.sid)             


-- elpased 59 secs
/* !!!!!!!!!!!!!!!! CHANGE ALSO THE MERGE IN SQLSTATS SNAP TO INCLUDE WHEN MATCHED
*/
with q1 as -- get sql statistics
(
select * from v$sqlstats
),
q2 as -- get only unique pairs (sid, sql_id) from session snapshot
(
select sid, sql_id 
from (
        select  ssnp.* 
                ,row_number() over(partition by ssnp.SID ,ssnp.SQL_ID ,ssnp.LOGON_TIME order by ssnp.POPULATION_DATE Desc ) as R 
        from  monitor_dw.mondw_vsession_snap ssnp
      ) 
where R = 1
)-- get the sql statistics for the sessions stored in session snap (this resutl will be merged on the sqlstats snapshot - for active sessions right now the elasped time will not be accurate)
select q1.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime, count(*) over()
from
    q1 JOIN q2 ON (q1.sql_id = q2.sql_id)

                  
--------------------------------

select status, count(*)
from v$session 
group by status


with q1 as
(
select * from v$sqlstats
),
q2 as
(
select sid
from v$session where status = 'ACTIVE'
),
q3 as
(
select sid, sql_id
from (
        select  ssnp.* 
                ,row_number() over(partition by ssnp.SID ,ssnp.SQL_ID ,ssnp.LOGON_TIME order by ssnp.POPULATION_DATE Desc ) as R 
        from  monitor_dw.mondw_vsession_snap ssnp
      ) 
where R = 1
),
q4 as -- anti-join between q4 & q2
(
select q3.sid, q3.sql_id 
from q3 left outer join q2 on(q3.sid = q2.sid)
where  q2.sid is null
)
select q1.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime, count(*) over()
from
    q1 JOIN q4 ON (q1.sql_id = q4.sql_id)










-- elapsed ???
with q1 as
(
select * from v$sqlstats
),
q2 as
(
SELECT NVL (SID,
                        (SELECT SID
                           FROM monitor_dw.mondw_vsession_snap
                          WHERE ROWNUM = 1))
              FROM v$session
             WHERE status = 'ACTIVE'
)
select q1.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime, count(*) over()
from
(q1 JOIN monitor_dw.mondw_vsession_snap sessionsnp
      ON (q1.sql_id = sessionsnp.sql_id)
)      
join q2 on(sessionsnp.sid <> q2.sid)                                           












MERGE INTO monitor_dw.mondw_vsqlstats_snap sqlsnp
   USING (SELECT s.*, SYSDATE, (SELECT startup_time
                                  FROM v$instance) AS dbuptime
            FROM v$sqlstats s JOIN monitor_dw.mondw_vsession_snap sessionsnp
                 ON (s.sql_id = sessionsnp.sql_id)
           WHERE sessionsnp.SID NOT IN (
                       SELECT NVL (SID,
                                   (SELECT SID
                                      FROM monitor_dw.mondw_vsession_snap
                                     WHERE ROWNUM = 1))
                         FROM v$session
                        WHERE status = 'ACTIVE')) sqlstats
   ON (sqlstats.sql_id = sqlsnp.sql_id AND sqlstats.sql_text = sqlsnp.sql_text)
   WHEN NOT MATCHED THEN
      INSERT (sqlsnp.instance_startup_date, sqlsnp.population_date,
              sqlsnp.typecheck_mem, sqlsnp.total_sharable_mem,
              sqlsnp.sharable_mem, sqlsnp.sorts, sqlsnp.java_exec_time,
              sqlsnp.plsql_exec_time, sqlsnp.user_io_wait_time,
              sqlsnp.cluster_wait_time, sqlsnp.concurrency_wait_time,
              sqlsnp.application_wait_time, sqlsnp.avg_hard_parse_time,
              sqlsnp.elapsed_time, sqlsnp.cpu_time,
              sqlsnp.px_servers_executions, sqlsnp.invalidations,
              sqlsnp.version_count, sqlsnp.loads, sqlsnp.end_of_fetch_count,
              sqlsnp.executions, sqlsnp.fetches, sqlsnp.serializable_aborts,
              sqlsnp.rows_processed, sqlsnp.buffer_gets, sqlsnp.direct_writes,
              sqlsnp.disk_reads, sqlsnp.parse_calls, sqlsnp.plan_hash_value,
              sqlsnp.last_active_child_address, sqlsnp.last_active_time,
              sqlsnp.sql_id, sqlsnp.sql_fulltext, sqlsnp.sql_text)
      VALUES (sqlstats.dbuptime, SYSDATE, sqlstats.typecheck_mem,
              sqlstats.total_sharable_mem, sqlstats.sharable_mem,
              sqlstats.sorts, sqlstats.java_exec_time,
              sqlstats.plsql_exec_time, sqlstats.user_io_wait_time,
              sqlstats.cluster_wait_time, sqlstats.concurrency_wait_time,
              sqlstats.application_wait_time, sqlstats.avg_hard_parse_time,
              sqlstats.elapsed_time, sqlstats.cpu_time,
              sqlstats.px_servers_executions, sqlstats.invalidations,
              sqlstats.version_count, sqlstats.loads,
              sqlstats.end_of_fetch_count, sqlstats.executions,
              sqlstats.fetches, sqlstats.serializable_aborts,
              sqlstats.rows_processed, sqlstats.buffer_gets,
              sqlstats.direct_writes, sqlstats.disk_reads,
              sqlstats.parse_calls, sqlstats.plan_hash_value,
              sqlstats.last_active_child_address, sqlstats.last_active_time,
              sqlstats.sql_id, sqlstats.sql_fulltext, sqlstats.sql_text)
              
              
              
              select *
              from MONITOR_DW.MONDW_USERS
              
              select *
              from TARGET_DW.PROVIDER_DIM 
              