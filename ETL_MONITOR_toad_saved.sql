
------------------------------------- Queries for everyday troubleshooting ------------------------------------------------

-- check if NMR has finished for today
select DWADMIN.CONC_STATS_COLLECTION.NMR_HAS_FINISHED from dual;

/*
Check flows control dates
*/

select *
from stage_dw.dw_control_table
order by 1

select *
from STAGE_PERIF.FLOW_PROGRESS_STG
order by 1

/*
    Check if KPIDW_MAIN is ready to run or if it is waiting for some other flow and which one
*/
with kpi as
(
SELECT RUN_DATE        
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'NMR_GLOBAL_RUN_DATE'
),
kpi_run as (
SELECT RUN_DATE        
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'NMR_GLOBAL_END_DATE'
)
select
case when
    (select run_date from kpi) >=
    (
    -- DAILY DWH      
      SELECT RUN_DATE + 1        
        FROM STAGE_DW.DW_CONTROL_TABLE        
        WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN'
    ) then    'WAITING'    ELSE    'OK'   
end LEVEL0_DAILY,
case when
    (select run_date from kpi) >=
    (
-- ΠΕΡΙΦΕΡΕΙΕΣ      
  SELECT FLOW_BASEDATE           
    FROM STAGE_PERIF.FLOW_PROGRESS_STG        
    WHERE FLOW_NAME = 'PERIF_NMR_END'    ) 
    then    'WAITING'    ELSE    'OK'   
end PER_MAIN,
case when
    (select run_date from kpi) >=
    (
-- SIEBEL    
  SELECT RUN_DATE + 1      
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE'
    )
    then    'WAITING'    ELSE    'OK'
end SOC_DW_MAIN,
case when
    (select run_date from kpi) >=
    (
-- CCRM      
  SELECT RUN_DATE           
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'CCRM_RUN_DATE'
    )
    then    'WAITING'    ELSE    'OK'
end CCRM,
case when
    (select run_date from kpi_run) >  sysdate
    then    'NMR Execution in progress'    ELSE    'NOT currently executing or Waiting'
end KPIDW_MAIN                                   
from dual;


/*
    Find all major flows executing NOW
*/
select /*+ parallel(a 32) */ *
from owbsys.all_rt_audit_executions a
where A.PARENT_EXECUTION_AUDIT_ID is NULL
AND A.EXECUTION_AUDIT_STATUS = 'BUSY'

/*
    Find mappings executing NOW for a specific Major Flow    
*/
SELECT  execution_audit_id,
           SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
           DECODE (task_type,
                   'PLSQL', 'Mapping',
                   'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
              TYPE,
           execution_audit_status,
           created_on,
           updated_on,
           ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
           CONNECT_BY_ISLEAF "IsLeaf",
           LEVEL,
           SYS_CONNECT_BY_PATH (
              SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
              '/')
              "Path",
           CONNECT_BY_ROOT execution_name root_execution_name,           
           CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
           CONNECT_BY_ROOT created_on root_created_on,
           CONNECT_BY_ROOT updated_on root_updated_on
FROM owbsys.all_rt_audit_executions a
WHERE   A.CREATED_ON > SYSDATE - (&days_back)
    AND a.execution_audit_status = 'BUSY'    --AND a.return_result = 'OK'
    AND CONNECT_BY_ISLEAF = 1
    AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
START WITH  a.PARENT_EXECUTION_AUDIT_ID IS NULL
            AND a.EXECUTION_NAME  = nvl(trim('&major_flow_name'), a.EXECUTION_NAME) 
            AND a.execution_audit_status <> 'COMPLETE'            --AND a.return_result = 'OK'
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id;

/*
Find mappings on error for a specific main flow (i.e., return result <> OK)
*/
SELECT  execution_audit_id,
           SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
           DECODE (task_type,
                   'PLSQL', 'Mapping',
                   'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
              TYPE,
           b.CREATION_DATE error_creation_date,
           execution_audit_status,
           a.RETURN_RESULT,
           b.SEVERITY,
           c.PLAIN_TEXT,           
           created_on,
           updated_on,
           ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
            owbsys.wb_rt_constants.to_string (severity),
              a.OBJECT_NAME,
              --a.TASK_OBJECT_STORE_NAME,
              a.TASK_NAME,
           CONNECT_BY_ISLEAF "IsLeaf",
           LEVEL,
           SYS_CONNECT_BY_PATH (
              SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
              '/')
              "Path",
           CONNECT_BY_ROOT execution_name root_execution_name,           
           CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
           CONNECT_BY_ROOT created_on root_created_on,
           CONNECT_BY_ROOT updated_on root_updated_on
FROM owbsys.all_rt_audit_executions a,   owbsys.WB_RT_AUDIT_MESSAGES b,
  owbsys.WB_RT_AUDIT_MESSAGE_LINES c
WHERE   A.CREATED_ON > SYSDATE - (&days_back)
    --AND a.execution_audit_status <> 'COMPLETE'
    AND a.return_result <> 'OK'
    AND CONNECT_BY_ISLEAF = 1
    AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
    AND a.EXECUTION_AUDIT_ID= b.AUDIT_EXECUTION_ID(+)  
    AND  b.AUDIT_MESSAGE_ID = c.AUDIT_MESSAGE_ID(+) 
START WITH  a.PARENT_EXECUTION_AUDIT_ID IS NULL
            AND a.EXECUTION_NAME  = nvl(trim('&major_flow_name'), a.EXECUTION_NAME) 
            AND a.execution_audit_status <> 'COMPLETE'            --AND a.return_result = 'OK'
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
order by b.CREATION_DATE desc

/***************
  Query to find the execution history of a specific node (leaf node or subflow) and remaining time until the end of main flow or other node
  
  The query returns the duration in minutes of the execution of the specific node in the history of interest.
  Also, it returns the minutes until the end of the Main Flow and the minutes until the end 
    of another node-milestone (e.g.SOC_FLOWPROGR_ENDSOC_ETL -- ενημέρωση ημερομηνίας ολοκλήρωσης SOC για NMR
                                , PERIF_FLOWPROGR_ENDSHADOW_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης shadow στη LEVEL0_DAILY) 
  
  Parameters:
  flow_name    (optional) The Main flow name e.g. SOC_DW_MAIN
  critical_ind  (optional)  0/1 if the main flow is critical
  days_back     (optional)  Num of days back from sysdate of required history
  mstone_node_name      (optional)  Name of a node milestone, other than the end of the Main flow
  node_name      Name of the node (leaf or subflow) for which the execution history we are interested
  monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
  Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays
****************/

with q1 as ( 
    -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
    select /*+ materialize dynamic_sampling (4) */  
        execution_audit_id, 
        parent_execution_audit_id,
        top_level_execution_audit_id,
        execution_name,
        task_type,
        created_on,
        updated_on,
        execution_audit_status,
        return_result
    from  owbsys.all_rt_audit_executions a
    where
        TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flows of interest and criticallity of interest
                                         select execution_audit_id
                                         from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                         where 
                                            execution_name = flow_name
                                            and PARENT_EXECUTION_AUDIT_ID IS NULL
                                            AND flow_name = nvl('&&flow_name',flow_name)
                                            AND critical_ind = nvl('&&critical_ind',critical_ind)
                                            -- restrictions for the main flow
                                            and created_on > sysdate - &&days_back
                                            --AND execution_audit_status = 'COMPLETE'
                                            --AND return_result = 'OK'                                                        
                                        )
    -- restricitons for all the nodes (not just the root)    
    AND CREATED_ON > SYSDATE - (&days_back)    
     AND to_char(created_on, 'DD') = case when nvl('&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
     AND trim(to_char(created_on, 'DAY')) = case when nvl('&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
   --AND a.execution_audit_status = 'COMPLETE'
   --AND a.return_result = 'OK'
)                                                                                          
select /*+ dynamic_sampling (4) */  
   CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
   execution_audit_id,
   SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
   DECODE (task_type,
           'PLSQL', 'Mapping',
           'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)    TYPE,
   created_on,
   updated_on, execution_audit_status,  return_result,
   ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
   ROUND((
            (   select c.updated_on 
                from owbsys.all_rt_audit_executions c 
                where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                      AND  c.execution_name like trim('%'||nvl('&mstone_node_name','xxxxxx'))
            ) 
        - created_on) * 24 * 60, 1) ms_unt_end_of_mstone_incl_node,
   ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) ms_until_end_of_root_incl_node,   
   CONNECT_BY_ROOT execution_name root_execution_name,
   --b.critical_ind,
   ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
   CONNECT_BY_ROOT created_on root_created_on,
   CONNECT_BY_ROOT updated_on root_updated_on,
   CONNECT_BY_ISLEAF "IsLeaf",
   LEVEL,
   SYS_CONNECT_BY_PATH (
      SUBSTR (execution_name, INSTR (execution_name, ':') + 1),      '/') "Path"  
from q1 --, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
WHERE 
   --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
   execution_name like trim('%&node_name')
   --AND CONNECT_BY_ISLEAF = 1 
   --AND execution_audit_status = 'COMPLETE'
   --AND return_result = 'OK'  
   --------AND CONNECT_BY_ROOT execution_name = b.flow_name
START WITH PARENT_EXECUTION_AUDIT_ID IS NULL
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;




/****************
    Query to find the execution of all major flows in the history of interest
    
  Parameters:
  flow_name    (optional) The Main flow name e.g. SOC_DW_MAIN
  critical_ind  (optional)  0/1 if the main flow is critical
  days_back     (optional)  Num of days back from sysdate of required history
  monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
  Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays    
    
*****************/

select 
  created_on,
   updated_on,
   ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
   case when execution_name = 'KPIDW_MAIN'
            THEN  round( ( updated_on - (  select updated_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'KPIDW_MAIN:CHECK_FLOWSEND_FORGLOBAL_PROC'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'
                                        ) 
                          )*24  -- hours
                  ,1)
        when execution_name = 'CTO_MAIN'
            THEN  round( ( updated_on - (  select updated_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'CTO_MAIN:CHECK_FLOWSEND_FORCTO_PROC'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'
                                        ) 
                          )*24  -- hours
                  ,1)                  
        when execution_name = 'SOC_DW_MAIN'
            THEN  round( ( updated_on - created_on
                                    - (  select updated_on - created_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'                                        
                                        )
                                      - (   select updated_on - created_on 
                                            from owbsys.all_rt_audit_executions tt 
                                            where
                                                tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                                                AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                                                                            
                                      ) 
                                      - (   select updated_on - created_on 
                                            from owbsys.all_rt_audit_executions tt 
                                            where
                                                tt.execution_name like 'SOC_FCTS_TRG:CHECK_FAULT_END'
                                                AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'   )                                                                                                                                               
                          )*24  -- hours
                  ,1)                   
        when execution_name = 'LEVEL0_DAILY' 
            THEN  round( ( (  select updated_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'LEVEL0_DAILY:LEVEL1_FINALIZE'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'                                  
                           )
                          - created_on
                          - nvl((select updated_on - created_on 
                          from owbsys.all_rt_audit_executions tt 
                          where
                              tt.execution_name like 'ADSL_SPEED_PRESTAGE:CHECK_NISA_SOURCE'
                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                              AND execution_audit_status = 'COMPLETE'
                              AND return_result = 'OK'                                  
                              ),0)                              
                          )*24  -- hours
                  ,1)
        when execution_name = 'PER_MAIN' 
            THEN  round( ( updated_on
                              - created_on
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_MAIN:STAGE_CHECK_DWH_ENDSHADOW_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_LOADSHADOW_DIM:CHECK_DWH_ENDSHADOW_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_MAIN:CHECK_DWH_ENDTARGET_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_LOADPRESTAGE_PROM:PRESTAGE_CHECKPROM_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )                                                                                                      
                          )*24  -- hours
                  ,1)
        when execution_name = 'CMP_RUN_DAILY' 
            THEN  round( ( updated_on - created_on
                              - (select sum(updated_on - created_on) 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'CMP_RUN_DAILY:PRERUNCHECK'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'FAULT_FCTS_TRG:CHECK_FLOW_WFM_FINNISH'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )
                          )*24  -- hours
                  ,1)
        when execution_name = 'OSM_RUN' 
                    THEN  round( ( updated_on - created_on
                                      - (select sum(updated_on - created_on) 
                                      from owbsys.all_rt_audit_executions tt 
                                      where
                                          tt.execution_name like 'OSM_RUN:PRERUNCHECK'
                                          AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                          AND execution_audit_status = 'COMPLETE'
                                          AND return_result = 'OK'                                  
                                          )                                      
                                  )*24  -- hours
                          ,1)
        when execution_name = 'WFM_RUN' 
                    THEN  round( ( updated_on - created_on
                                      - (select sum(updated_on - created_on) 
                                      from owbsys.all_rt_audit_executions tt 
                                      where
                                          tt.execution_name like 'WFM_RUN:PRERUNCHECK'
                                          AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                          AND execution_audit_status = 'COMPLETE'
                                          AND return_result = 'OK'                                  
                                          )                                      
                                  )*24  -- hours
                          ,1)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
        ELSE null
    END duration_real_hrs, -- afairoume tous xronous anamonhs                         
   execution_name,
   b.critical_ind
from owbsys.all_rt_audit_executions a, monitor_dw.dwp_etl_flows b
where
    a.execution_name = flow_name
    AND flow_name = nvl('&&flow_name',flow_name)
    AND critical_ind = nvl('&&critical_ind',critical_ind)
    and created_on > sysdate - &&days_back
    AND to_char(created_on, 'DD') = case when nvl('&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
    AND trim(to_char(created_on, 'DAY')) = case when nvl('&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
    AND execution_audit_status = 'COMPLETE'
    AND return_result = 'OK'
order by  execution_name, CREATED_ON desc        

/*
 Find the procedure if the node type is procedure
*/
select *
from dba_procedures
where
 owner in ('ETL_DW', 'PERIF') 
 and procedure_name in ('&pname')
 
/*
    Find the package if the type is "mapping"
    
    Note: might need to remove the "_1" from the end of the mapping name to match with the package name
        
*/ 

 select *
 from dba_objects
 where
  owner in ('ETL_DW', 'PERIF')
  and 
  object_name like '%&mapping_name%'
  and object_type = 'PACKAGE'

------------------------------------- PERFORMANCE ANALYSIS Queries ------------------------------------------------

/* 
    Find execution history of the top N heaviest mappings of the last run(or for a specific date) for a specific flow
    and then get the history of runs from that date back to days_back,in order to spot deviations from normal execution duration.
    If you leave "execution_date" null it will base on the most recent run 
    
    The query ranks the mappings of a specific execution (execution for a specific execution_date) based on the duration of each mapping
    (rank 1 is the longest). Then for the specific set of N mappingsit return the history of executions from the specific date back to
    "days_back".
*/ 
select  HIST.TOP_LEVEL_EXECUTION_AUDIT_ID,
        hist.execution_audit_id, --hist.execution_name,
        top.ranking,
       SUBSTR (hist.execution_name, INSTR (hist.execution_name, ':') + 1) execution_name_short,
       DECODE (hist.task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
          TYPE,
       hist.created_on,
       hist.updated_on,
       ROUND ( (hist.updated_on - hist.created_on) * 24 * 60, 1) duration_mins 
from 
( -- find the top N heaviest mappings in the most recent execution 
    select rownum ranking, tt.*
    from (
    /*
         Query to find the execution of all mappings or procedures and functions for each parent ETL Flow of interest
        and criticallity of interest and history of interest
    */
    with q1 as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
            execution_name,
            task_type,
            created_on,
            updated_on
        from  owbsys.all_rt_audit_executions a
        where
            TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flow of interest and criticallity of interest(get the last run (sysdate - 1))
                                             select  execution_audit_id
                                             from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                             where 
                                                execution_name = flow_name
                                                and PARENT_EXECUTION_AUDIT_ID IS NULL
                                                AND flow_name = nvl('&&flow_name',flow_name)
                                                AND critical_ind = nvl('&&critical_ind',critical_ind)
                                                -- restrictions for the main flow
                                                and trunc(created_on) = nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) --created_on > sysdate - 1
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                        
                                            )
        -- restricitons for all the nodes (not just the root)    
        AND trunc(created_on) = nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) --CREATED_ON > SYSDATE - 1
       AND a.execution_audit_status = 'COMPLETE'
       AND a.return_result = 'OK'
    )                                                                                           
    select /*+ dynamic_sampling (4) */  
       CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
        execution_audit_id,
        execution_name,
       SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
       DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
          TYPE,
       created_on,
       updated_on,
       ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
       ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) mins_until_end_of_root,
       CONNECT_BY_ROOT execution_name root_execution_name,
       --b.critical_ind,
       ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
       CONNECT_BY_ROOT created_on root_created_on,
       CONNECT_BY_ROOT updated_on root_updated_on,
       CONNECT_BY_ISLEAF "IsLeaf",
       LEVEL,
       SYS_CONNECT_BY_PATH (
          SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
          '/')
          "Path"  
    from q1 a--, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
    WHERE 
       task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
       AND CONNECT_BY_ISLEAF = 1   
       --AND CONNECT_BY_ROOT execution_name = b.flow_name
    START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
     ORDER BY  duration_mins DESC--root_execution_name, root_execution_audit_id DESC, duration_mins DESC
    ) tt
    where
        rownum <= &N 
) top,
 owbsys.all_rt_audit_executions hist
where
    top.execution_name = hist.execution_name
    --top.execution_audit_id = HIST.EXECUTION_AUDIT_ID
    --AND top.root_execution_audit_id = HIST.TOP_LEVEL_EXECUTION_AUDIT_ID
    and hist.created_on > nvl(to_date('&&execution_date', 'dd/mm/yyyy'), sysdate) - &&days_back -- sysdate - &&days_back 
order by
    top.ranking asc, hist.execution_name, hist.created_on desc;

/*
    Find the top N heaviest mappings in the last"days_back" days from a specific date (execution_date) for a specific top-level flow  
    (exclude "%CHECK%" nodes)
    
    INPUT
        * flow_name: name of the flow of interest (if null, it runs for all top level flows)
        * critical_ind:  0,1 (indication if flow is critical or no, according to table monitor_dw.dwp_etl_flows)
        * execution_date: date milestone from which we want to check X days back (if left  null then it uses sysdate),
                         in the form dd/mm/yyyy
        * days_back: how many days back you want to check execution history
        * N : define the "N" for top N

    Example
        Show me the top 10 heaviest mappings of SOC_DW_MAIN for all executions 30 days back from 12/03/2014
        
        flow_name:  SOC_DW_MAIN
        critical_ind: 1
        execution_date: 12/03/2014
        days_back: 30
        N: 10          
*/

    select rownum ranking, tt.*
    from (
        -- Query to find the execution of all mappings or procedures and functions for each parent ETL Flow of interest
        --and criticality of interest and history of interest
    with q1 as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
            execution_name,
            task_type,
            created_on,
            updated_on
        from  owbsys.all_rt_audit_executions a
        where
            TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flow of interest and criticality of interest(get the last run (sysdate - 1))
                                             select  execution_audit_id
                                             from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                             where 
                                                execution_name = flow_name
                                                and PARENT_EXECUTION_AUDIT_ID IS NULL
                                                AND flow_name = nvl('&&flow_name',flow_name)
                                                AND critical_ind = nvl('&&critical_ind',critical_ind)
                                                -- restrictions for the main flow
                                                and trunc(created_on) > nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) - &&days_back --created_on > sysdate - 1                                                
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                        
                                            )
        -- restricitons for all the nodes (not just the root)    
        --AND trunc(created_on) = nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) --CREATED_ON > SYSDATE - 1
       and trunc(created_on) > nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) - &days_back 
       AND a.execution_audit_status = 'COMPLETE'
       AND a.return_result = 'OK'
    )                                                                                           
    select /*+ dynamic_sampling (4) */  
       CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
        execution_audit_id,
        execution_name,
       SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
       DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
          TYPE,
       created_on,
       updated_on,
       ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
       ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) mins_until_end_of_root,
       CONNECT_BY_ROOT execution_name root_execution_name,
       --b.critical_ind,
       ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
       CONNECT_BY_ROOT created_on root_created_on,
       CONNECT_BY_ROOT updated_on root_updated_on,
       CONNECT_BY_ISLEAF "IsLeaf",
       LEVEL,
       SYS_CONNECT_BY_PATH (
          SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
          '/')
          "Path"  
    from q1 a--, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
    WHERE 
       task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
       AND CONNECT_BY_ISLEAF = 1   
       --AND CONNECT_BY_ROOT execution_name = b.flow_name
       AND SUBSTR (execution_name, INSTR (execution_name, ':') + 1) NOT LIKE '%CHECK%' -- exclude "check" nodes
    START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
     ORDER BY  duration_mins DESC--root_execution_name, root_execution_audit_id DESC, duration_mins DESC
    ) tt
    where
        rownum <= &N; 


/*
     Query to find the execution of all mappings or procedures and functions for each parent ETL Flow of interest
    and criticallity of interest and history of interest
*/
with q1 as ( 
    -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
    select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
        execution_name,
        task_type,
        created_on,
        updated_on
    from  owbsys.all_rt_audit_executions a
    where
        TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flows of interest and criticallity of interest
                                         select execution_audit_id
                                         from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                         where 
                                            execution_name = flow_name
                                            and PARENT_EXECUTION_AUDIT_ID IS NULL
                                            AND flow_name = nvl('&&flow_name',flow_name)
                                            AND critical_ind = nvl('&&critical_ind',critical_ind)
                                            -- restrictions for the main flow
                                            and created_on > sysdate - &&days_back
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                                        
                                        )
    -- restricitons for all the nodes (not just the root)    
    AND CREATED_ON > SYSDATE - (&days_back)
   AND a.execution_audit_status = 'COMPLETE'
   AND a.return_result = 'OK'
)                                                                                           
select /*+ dynamic_sampling (4) */  
   CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
    execution_audit_id,
   SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
   DECODE (task_type,
           'PLSQL', 'Mapping',
           'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
      TYPE,
   created_on,
   updated_on,
   ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
   ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) mins_until_end_of_root,
   CONNECT_BY_ROOT execution_name root_execution_name,
   --b.critical_ind,
   ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
   CONNECT_BY_ROOT created_on root_created_on,
   CONNECT_BY_ROOT updated_on root_updated_on,
   CONNECT_BY_ISLEAF "IsLeaf",
   LEVEL,
   SYS_CONNECT_BY_PATH (
      SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
      '/')
      "Path"  
from q1 a--, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
WHERE 
   task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
   AND CONNECT_BY_ISLEAF = 1   
   --AND CONNECT_BY_ROOT execution_name = b.flow_name
START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
 ORDER BY root_execution_name, root_execution_audit_id DESC, duration_mins DESC





/*
  Alternative  Query to find the execution history of a specific node
*/
select execution_name, creation_date, last_update_date,  ROUND ( (last_update_date - creation_date) * 24 * 60, 1) duration_mins
, wb_rt_constants.to_string (audit_status) status, return_result
from owbsys.wb_rt_audit_executions t
where execution_name like trim('%&node_name')
and creation_date > sysdate - (&days_back)
order by creation_date desc





 
/*
 -- pcnt that KPIDW_MAIN starts before 07:00 am (before and after Siebel deployment
*/
select round(sum(case when
            NUMTODSINTERVAL(to_char(updated_on,'hh24'),'hour') < NUMTODSINTERVAL(7, 'HOUR') and  updated_on > date'2014-03-12' 
       then 1
       else 0
       end) / sum(1) * 100)||'%'  pcnt_after,
    round(sum (case when
            NUMTODSINTERVAL(to_char(updated_on,'hh24'),'hour') < NUMTODSINTERVAL(7, 'HOUR') and  updated_on < date'2014-03-12' 
       then 1
       else 0
       end) / sum(1) * 100)||'%' pcnt_before        
from (
    with q1 as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
            execution_name,
            task_type,
            created_on,
            updated_on
        from  owbsys.all_rt_audit_executions a
        where
            TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flows of interest and criticallity of interest
                                             select execution_audit_id
                                             from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                             where 
                                                execution_name = flow_name
                                                and PARENT_EXECUTION_AUDIT_ID IS NULL
                                                AND flow_name = nvl('&&flow_name',flow_name)
                                                AND critical_ind = nvl('&&critical_ind',critical_ind)
                                                -- restrictions for the main flow
                                                and created_on > sysdate - &&days_back
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                        
                                            )
        -- restricitons for all the nodes (not just the root)    
        AND CREATED_ON > SYSDATE - (&days_back)
       AND a.execution_audit_status = 'COMPLETE'
       AND a.return_result = 'OK'
    )                                                                                           
    select /*+ dynamic_sampling (4) */  
       CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
        execution_audit_id,
       SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
       DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
          TYPE,
       created_on,
       updated_on,
       ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
       ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) mins_until_end_of_root,
       CONNECT_BY_ROOT execution_name root_execution_name,
       --b.critical_ind,
       ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
       CONNECT_BY_ROOT created_on root_created_on,
       CONNECT_BY_ROOT updated_on root_updated_on,
       CONNECT_BY_ISLEAF "IsLeaf",
       LEVEL,
       SYS_CONNECT_BY_PATH (
          SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
          '/')
          "Path"  
    from q1 a--, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
    WHERE 
       --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
       execution_name like trim('%&node_name')
       AND CONNECT_BY_ISLEAF = 1   
       --AND CONNECT_BY_ROOT execution_name = b.flow_name
    START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id 
) t
 
/*
 PCTN that CTO_MAIN is completed before 09:00
*/
select  sum(case when 
            NUMTODSINTERVAL(to_char(updated_on,'hh24'),'hour') <= NUMTODSINTERVAL(9, 'HOUR') then 1
            ELSE 0
        END) cnt_sla_ok,
        sum(1) cnt_total,
        round(sum(case when 
            NUMTODSINTERVAL(to_char(updated_on,'hh24'),'hour') < NUMTODSINTERVAL(9, 'HOUR') then 1
            ELSE 0
        END)/sum(1) * 100) pct
from (
select 
  created_on,
   updated_on,
   ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
   case when execution_name = 'KPIDW_MAIN'
            THEN  round( ( updated_on - (  select updated_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'KPIDW_MAIN:CHECK_FLOWSEND_FORGLOBAL_PROC'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'
                                        ) 
                          )*24  -- hours
                  ,1)
        when execution_name = 'CTO_MAIN'
            THEN  round( ( updated_on - (  select updated_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'CTO_MAIN:CHECK_FLOWSEND_FORCTO_PROC'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'
                                        ) 
                          )*24  -- hours
                  ,1)                  
        when execution_name = 'SOC_DW_MAIN'
            THEN  round( ( updated_on - (  select updated_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'                                        
                                        )
                                      - (   select updated_on - created_on 
                                            from owbsys.all_rt_audit_executions tt 
                                            where
                                                tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                                                AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                                                                            
                                      ) 
                          )*24  -- hours
                  ,1)                   
        when execution_name = 'LEVEL0_DAILY' 
            THEN  round( ( (  select updated_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'LEVEL0_DAILY:LEVEL1_FINALIZE'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'                                  
                           )
                          - created_on
                          - nvl((select updated_on - created_on 
                          from owbsys.all_rt_audit_executions tt 
                          where
                              tt.execution_name like 'ADSL_SPEED_PRESTAGE:CHECK_NISA_SOURCE'
                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                              AND execution_audit_status = 'COMPLETE'
                              AND return_result = 'OK'                                  
                              ),0)                              
                          )*24  -- hours
                  ,1)
        when execution_name = 'PER_MAIN' 
            THEN  round( ( updated_on
                              - created_on
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_MAIN:STAGE_CHECK_DWH_ENDSHADOW_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_LOADSHADOW_DIM:CHECK_DWH_ENDSHADOW_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_MAIN:CHECK_DWH_ENDTARGET_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_LOADPRESTAGE_PROM:PRESTAGE_CHECKPROM_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )                                                                                                      
                          )*24  -- hours
                  ,1)
        when execution_name = 'CMP_RUN_DAILY' 
            THEN  round( ( updated_on - created_on
                              - (select sum(updated_on - created_on) 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'CMP_RUN_DAILY:PRERUNCHECK'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'FAULT_FCTS_TRG:CHECK_FLOW_WFM_FINNISH'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )
                          )*24  -- hours
                  ,1)                                                                                                                                                                                                                                                                                                                                                                                       
        ELSE null
    END duration_real_hrs, -- afairoume tous xronous anamonhs                         
   execution_name,
   b.critical_ind
from owbsys.all_rt_audit_executions a, monitor_dw.dwp_etl_flows b
where
    a.execution_name = flow_name
    AND flow_name = nvl('&&flow_name',flow_name)
    AND critical_ind = nvl('&&critical_ind',critical_ind)
    and created_on > sysdate - &&days_back
    AND execution_audit_status = 'COMPLETE'
    AND return_result = 'OK'
) t
-------------------------------- OLDER VERSIONS of the above queries ----------------------

/*
    **OLD VERSION**
    
  Query to find the execution history of a specific node
 ************* Shows also the remaining time until the end of the root flow*****************
*/
with q1 as ( 
    -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
    select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
        execution_name,
        task_type,
        created_on,
        updated_on,
        execution_audit_status,
        return_result
    from  owbsys.all_rt_audit_executions a
    where
        TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flows of interest and criticallity of interest
                                         select execution_audit_id
                                         from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                         where 
                                            execution_name = flow_name
                                            and PARENT_EXECUTION_AUDIT_ID IS NULL
                                            AND flow_name = nvl('&&flow_name',flow_name)
                                            AND critical_ind = nvl('&&critical_ind',critical_ind)
                                            -- restrictions for the main flow
                                            and created_on > sysdate - &&days_back
                                            --AND execution_audit_status = 'COMPLETE'
                                            --AND return_result = 'OK'                                                        
                                        )
    -- restricitons for all the nodes (not just the root)    
    AND CREATED_ON > SYSDATE - (&days_back)
   --AND a.execution_audit_status = 'COMPLETE'
   --AND a.return_result = 'OK'
)                                                                                           
select /*+ dynamic_sampling (4) */  
   CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
    execution_audit_id,
   SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
   DECODE (task_type,
           'PLSQL', 'Mapping',
           'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
      TYPE,
   created_on,
   updated_on, execution_audit_status,  return_result,
   ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
   ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) ms_until_end_of_root_incl_node,
   CONNECT_BY_ROOT execution_name root_execution_name,
   --b.critical_ind,
   ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
   CONNECT_BY_ROOT created_on root_created_on,
   CONNECT_BY_ROOT updated_on root_updated_on,
   CONNECT_BY_ISLEAF "IsLeaf",
   LEVEL,
   SYS_CONNECT_BY_PATH (
      SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
      '/')
      "Path"  
from q1 a--, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
WHERE 
   --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
   execution_name like trim('%&node_name')
   AND CONNECT_BY_ISLEAF = 1 
   --AND execution_audit_status = 'COMPLETE'
   --AND return_result = 'OK'  
   --------AND CONNECT_BY_ROOT execution_name = b.flow_name
START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
 ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
    
/*
    **** OLD VERSION - Obsolete (use the above single query for both dailty and monthly)
    
   Query to find the execution of all major flows (for the 1st of month only) for the last 10 executions
*/
select round(avg (duration_real_hrs) over(partition by execution_name),1) avg_duration_real_hrs, t.*
from (
select 
  created_on,
   updated_on,
   ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
   case when execution_name = 'KPIDW_MAIN'
            THEN  round( ( updated_on - (  select updated_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'KPIDW_MAIN:CHECK_FLOWSEND_FORGLOBAL_PROC'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'
                                        ) 
                          )*24  -- hours
                  ,1)
        when execution_name = 'SOC_DW_MAIN'
            THEN  round( ( updated_on - (  select updated_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'                                        
                                        )
                                      - (   select updated_on - created_on 
                                            from owbsys.all_rt_audit_executions tt 
                                            where
                                                tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                                                AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                                                                            
                                      ) 
                          )*24  -- hours
                  ,1)                   
        when execution_name = 'LEVEL0_DAILY' 
            THEN  round( ( (  select updated_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'LEVEL0_DAILY:LEVEL1_FINALIZE'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'                                  
                           )
                           - created_on
                          - nvl((select updated_on - created_on 
                          from owbsys.all_rt_audit_executions tt 
                          where
                              tt.execution_name like 'ADSL_SPEED_PRESTAGE:CHECK_NISA_SOURCE'
                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                              AND execution_audit_status = 'COMPLETE'
                              AND return_result = 'OK'                                  
                              ),0)                                                         
                          )*24  -- hours
                  ,1)
        when execution_name = 'PER_MAIN' 
            THEN  round( ( updated_on
                              - created_on
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_MAIN:STAGE_CHECK_DWH_ENDSHADOW_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_LOADSHADOW_DIM:CHECK_DWH_ENDSHADOW_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_MAIN:CHECK_DWH_ENDTARGET_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'PER_LOADPRESTAGE_PROM:PRESTAGE_CHECKPROM_PROC'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )                                                                                                      
                          )*24  -- hours
                  ,1)   
        when execution_name = 'CMP_RUN_DAILY' 
            THEN  round( ( updated_on - created_on
                              - (select sum(updated_on - created_on) 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'CMP_RUN_DAILY:PRERUNCHECK'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  )
                              - (select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'FAULT_FCTS_TRG:CHECK_FLOW_WFM_FINNISH'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                   )
                          )*24  -- hours
                  ,1)                                                                                                                                                                                                                                                   
        ELSE null
    END duration_real_hrs, -- afairoume tous xronous anamonhs                         
   execution_name,
   b.critical_ind
from (select * from owbsys.all_rt_audit_executions where to_char(created_on, 'DD') = '01') a, monitor_dw.dwp_etl_flows b
where
    a.execution_name = flow_name
    AND flow_name = nvl('&&flow_name',flow_name)
    AND critical_ind = nvl('&&critical_ind',critical_ind)
    AND execution_audit_status = 'COMPLETE'
    AND return_result = 'OK'
order by  execution_name, CREATED_ON desc 
) t
where
rownum <= (select count(*) from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind)) * 10      
 
 ------------------------------------------------------------------------------------------------------------------------------
 
 
/*Το παρακάτω script δειχνει τα mappings που έχουν τρέξει με διάφορες πληροφορίες (χρόνους, λάθη, κατάσταση κτλ)*/ 
  
SELECT   a.created_by, a.rta_lob_name, a.creation_date, a.last_update_date, 
         a.rta_elapse, a.rta_select, a.rta_insert, a.rta_update, a.rta_delete, 
         a.rta_merge, a.rta_errors, a.rta_status, b.rte_dest_table, 
         b.rte_sqlerrm 
    FROM owbsys.wb_rt_audit a, owbsys.wb_rt_errors b 
   WHERE a.rta_iid = b.rta_iid(+) 
     AND a.creation_date BETWEEN TO_DATE ('20090206 00:00:00','yyyymmdd hh24:mi:ss') 
                             AND TO_DATE ('20100101', 'yyyymmdd') 
     --AND a.rta_lob_name in ('ΧΧΧ') 
ORDER BY 4 Desc 

/*
ποια ροή «έσκασε», που και με ποιο Error message
*/
SELECT
  a.EXECUTION_NAME,
  a.RETURN_RESULT,
  a.CREATION_DATE,
  a.LAST_UPDATE_DATE,
  a.ELAPSE,
  TO_CHAR (TRUNC (SYSDATE, 'DD') + a.elapse / (24 * 3600), 'HH24:MI:SS') ,
  owbsys.wb_rt_constants.to_string (a.audit_status),
  /*NVL(WB_RT_AUDIT.RTA_ERRORS,0),
  NVL(WB_RT_AUDIT.RTA_SELECT,0),
  NVL(WB_RT_AUDIT.RTA_INSERT,0),
  NVL(WB_RT_AUDIT.RTA_UPDATE,0),
  NVL(WB_RT_AUDIT.RTA_DELETE,0),
  NVL(WB_RT_AUDIT.RTA_MERGE,0),
  NVL(WB_RT_AUDIT.RTA_CORRECTIONS,0),*/
  owbsys.wb_rt_constants.to_string (severity),
  WB_RT_AUDIT_MESSAGES.SEVERITY,
  WB_RT_AUDIT_MESSAGE_LINES.PLAIN_TEXT,
  WB_RT_AUDIT_MESSAGES.CREATION_DATE,
  a.TASK_OBJECT_NAME,
  a.TASK_OBJECT_STORE_NAME,
  a.TASK_NAME
FROM
  owbsys.WB_RT_AUDIT_EXECUTIONS a,
  owbsys.WB_RT_AUDIT_EXECUTIONS b,
  --owbsys.WB_RT_AUDIT,
  owbsys.WB_RT_AUDIT_MESSAGES,
  owbsys.WB_RT_AUDIT_MESSAGE_LINES
WHERE
A.TOP_LEVEL_AUDIT_EXECUTION_ID = B.AUDIT_EXECUTION_ID and B.EXECUTION_NAME like '%LEVEL0%'
and
  ( a.AUDIT_EXECUTION_ID=WB_RT_AUDIT_MESSAGES.AUDIT_EXECUTION_ID(+)  )
  AND  ( WB_RT_AUDIT_MESSAGES.AUDIT_MESSAGE_ID=WB_RT_AUDIT_MESSAGE_LINES.AUDIT_MESSAGE_ID(+)  )
--  AND  ( a.AUDIT_EXECUTION_ID=WB_RT_AUDIT.RTE_ID(+)  )
--  AND  
--  a.EXECUTION_NAME  LIKE  '%LEVEL0%'
 and a.EXECUTION_NAME not in ('LEVEL0_DAILY:DIMS_LOADING_VALIDATION', 'LEVEL0_DAILY_CATCHUP:CATCH_UP_NOT_FINISHED', 'LEVEL0_DAILY')
 and a.execution_name not like ('%CATCH_UP_NOT_FINISHED')
 and a.execution_name not like ('%MUST%RUN')
 and a.execution_name not like ('%LAST_DAY_OF_MONTH')
--AND   WB_RT_AUDIT.RTA_DATE  > trunc(sysdate - 30)
and a.CREATION_DATE > trunc(sysdate - 100) -- last 3 months
aNd a.return_result = 'FAILURE'
and  WB_RT_AUDIT_MESSAGE_LINES.PLAIN_TEXT is not null
order by  a.CREATION_DATE desc

-- for perif
SELECT
  a.EXECUTION_NAME,
  a.RETURN_RESULT,
  a.CREATION_DATE,
  a.LAST_UPDATE_DATE,
  a.ELAPSE,
  TO_CHAR (TRUNC (SYSDATE, 'DD') + a.elapse / (24 * 3600), 'HH24:MI:SS') ,
  owbsys.wb_rt_constants.to_string (a.audit_status),
  /*NVL(WB_RT_AUDIT.RTA_ERRORS,0),
  NVL(WB_RT_AUDIT.RTA_SELECT,0),
  NVL(WB_RT_AUDIT.RTA_INSERT,0),
  NVL(WB_RT_AUDIT.RTA_UPDATE,0),
  NVL(WB_RT_AUDIT.RTA_DELETE,0),
  NVL(WB_RT_AUDIT.RTA_MERGE,0),
  NVL(WB_RT_AUDIT.RTA_CORRECTIONS,0),*/
  owbsys.wb_rt_constants.to_string (severity),
  WB_RT_AUDIT_MESSAGES.SEVERITY,
  WB_RT_AUDIT_MESSAGE_LINES.PLAIN_TEXT,
  WB_RT_AUDIT_MESSAGES.CREATION_DATE,
  a.TASK_OBJECT_NAME,
  a.TASK_OBJECT_STORE_NAME,
  a.TASK_NAME
FROM
  owbsys.WB_RT_AUDIT_EXECUTIONS a,
  owbsys.WB_RT_AUDIT_EXECUTIONS b,
  --owbsys.WB_RT_AUDIT,
  owbsys.WB_RT_AUDIT_MESSAGES,
  owbsys.WB_RT_AUDIT_MESSAGE_LINES
WHERE
A.TOP_LEVEL_AUDIT_EXECUTION_ID = B.AUDIT_EXECUTION_ID and B.EXECUTION_NAME like  '%PER_MAIN%' --'%PERIF_MAIN%'
and
  ( a.AUDIT_EXECUTION_ID=WB_RT_AUDIT_MESSAGES.AUDIT_EXECUTION_ID(+)  )
  AND  ( WB_RT_AUDIT_MESSAGES.AUDIT_MESSAGE_ID=WB_RT_AUDIT_MESSAGE_LINES.AUDIT_MESSAGE_ID(+)  )
--  AND  ( a.AUDIT_EXECUTION_ID=WB_RT_AUDIT.RTE_ID(+)  )
--  AND  
--  a.EXECUTION_NAME  LIKE  '%LEVEL0%' 
 and a.execution_name not like ('%CREATE_MON_SNAPSHOT_CHECK_PROC')
 and a.execution_name not like ('%PRESTAGE_CHECKFLOW_PROC')
--AND   WB_RT_AUDIT.RTA_DATE  > trunc(sysdate - 30)
and a.CREATION_DATE > trunc(sysdate - 150) -- last 3 months
aNd a.return_result <> 'OK'
and  WB_RT_AUDIT_MESSAGE_LINES.PLAIN_TEXT is not null
order by  a.CREATION_DATE desc



/*
SELECT
  WB_RT_AUDIT_EXECUTIONS.EXECUTION_NAME,
  WB_RT_AUDIT_EXECUTIONS.RETURN_RESULT,
  WB_RT_AUDIT_EXECUTIONS.CREATION_DATE,
  WB_RT_AUDIT_EXECUTIONS.LAST_UPDATE_DATE,
  WB_RT_AUDIT_EXECUTIONS.ELAPSE,
  TO_CHAR (TRUNC (SYSDATE, 'DD') + elapse / (24 * 3600), 'HH24:MI:SS') ,
  wb_rt_constants.to_string (audit_status),
  NVL(WB_RT_AUDIT.RTA_ERRORS,0),
  NVL(WB_RT_AUDIT.RTA_SELECT,0),
  NVL(WB_RT_AUDIT.RTA_INSERT,0),
  NVL(WB_RT_AUDIT.RTA_UPDATE,0),
  NVL(WB_RT_AUDIT.RTA_DELETE,0),
  NVL(WB_RT_AUDIT.RTA_MERGE,0),
  NVL(WB_RT_AUDIT.RTA_CORRECTIONS,0),
  wb_rt_constants.to_string (severity),
  WB_RT_AUDIT_MESSAGES.SEVERITY,
  WB_RT_AUDIT_MESSAGE_LINES.PLAIN_TEXT,
  WB_RT_AUDIT_MESSAGES.CREATION_DATE,
  WB_RT_AUDIT_EXECUTIONS.TASK_OBJECT_NAME,
  WB_RT_AUDIT_EXECUTIONS.TASK_OBJECT_STORE_NAME,
  WB_RT_AUDIT_EXECUTIONS.TASK_NAME
FROM
  owbsys.WB_RT_AUDIT_EXECUTIONS,
  owbsys.WB_RT_AUDIT,
  owbsys.WB_RT_AUDIT_MESSAGES,
  owbsys.WB_RT_AUDIT_MESSAGE_LINES
WHERE
  ( WB_RT_AUDIT_EXECUTIONS.AUDIT_EXECUTION_ID=WB_RT_AUDIT_MESSAGES.AUDIT_EXECUTION_ID(+)  )
  AND  ( WB_RT_AUDIT_MESSAGES.AUDIT_MESSAGE_ID=WB_RT_AUDIT_MESSAGE_LINES.AUDIT_MESSAGE_ID(+)  )
  AND  ( WB_RT_AUDIT_EXECUTIONS.AUDIT_EXECUTION_ID=WB_RT_AUDIT.RTE_ID(+)  )
  AND  
  WB_RT_AUDIT_EXECUTIONS.EXECUTION_NAME  LIKE  '%LEVEL0%'
AND   WB_RT_AUDIT.RTA_DATE  > trunc(sysdate -3)
*/



/*
Monitor the executions of workflows. by setting PARENT_AUDIT_EXECUTION_ID is null we get only the parent workflows
*/
select * from owbsys.all_rt_audit_executions a 
where a.EXECUTION_NAME='LEVEL0_DAILY' 
--and   A.CREATED_ON>to_date('','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
-- and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC; 

select * from owbsys.all_rt_audit_executions a 
where a.EXECUTION_NAME like 'LEVEL1_FCT_ORD_4TARGET%' 
--and   A.CREATED_ON>to_date('','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
-- and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC; 



/*
How much time does it take to run the PER_MAIN flow in the last three months
*/
select EXECUTION_NAME, duration_hours, count(*) frequency
from (
select a.EXECUTION_NAME, a.CREATED_ON,  a.UPDATED_ON,  round((a.updated_on-a.created_on)*24 - (t.updated_on-t.created_on)*24, 1) duration_hours  
from owbsys.all_rt_audit_executions a,
(   -- komvos anamonis ths LEVEL0
    select B.TOP_LEVEL_EXECUTION_AUDIT_ID, CREATED_ON,  UPDATED_ON,  round((updated_on-created_on)*24, 1) duration_hours
    from owbsys.all_rt_audit_executions b
    where b.execution_name like 'PER_LOADSHADOW_DIM:CHECK_DWH_ENDSHADOW_PROC'
    and   b.CREATED_ON>=to_date('01/02/2012','dd/mm/yyyy') 
    and b.execution_audit_status='COMPLETE' and b.return_result='OK'
    ORDER BY b.CREATED_ON DESC
) t
where a.EXECUTION_NAME in ('PER_MAIN')
and   A.CREATED_ON>=to_date('01/02/2012','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
and A.TOP_LEVEL_EXECUTION_AUDIT_ID = t.TOP_LEVEL_EXECUTION_AUDIT_ID
-- and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME, duration_hours
order by frequency desc


/*
How much time does it take to run the LEVEL0_DAILY flow in the last three months
*/
select EXECUTION_NAME, duration, count(*)
from (
select a.EXECUTION_NAME, CREATED_ON, to_char(created_on, 'DAY') created_on_day,  UPDATED_ON,  round((updated_on-created_on)*24, 1) duration_hours  
from owbsys.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('LEVEL0_DAILY', 'LEVEL0_DAILY_CATCHUP:LEVEL0_DAILY', 'LEVEL0_DAILY_CATCHUP_OLD:LEVEL0_DAILY')--, '%DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' --
and   A.CREATED_ON> sysdate - interval '3' month  --to_date('01/03/2011','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
-- and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME, duration
order by 1, 2 asc

/*
Alternative for level0 from monitor_dw.log_flows (for up to the end of level0)
*/
select
    'LEVEL0',
    round(to_end_hrs) duration, 
    count(*) frequency            
from (
select owb_creation_date, to_char(owb_creation_date, 'DAY') owb_Creation_DAY, datatoload_date, round((target_end_date - owb_creation_date)*24, 2) to_target_hrs, round((etl_end_date-owb_creation_date)*24,2) to_end_hrs
from monitor_dw.log_flows
order by datatoload_date desc
)
group by
    'LEVEL0',
    round(to_end_hrs) 
order by frequency desc 

/*
 This view is intended for analysis (roll-ups, cube etc) over the log_flows
*/
create or replace view monitor_dw.v_analyze_log_flows
as
select  owb_creation_date, 
        to_char(owb_creation_date, 'DAY') owb_Creation_DAY,
        to_char(owb_creation_date, 'MONTH') owb_Creation_MONTH,
        datatoload_date, 
        to_char(datatoload_date, 'DAY') datatoload_date_DAY,
        to_char(datatoload_date, 'MONTH') datatoload_date_MONTH, 
        round((target_end_date - owb_creation_date)*24, 2) to_target_hrs, round((etl_end_date-owb_creation_date)*24,2) to_end_hrs,
        case when round((etl_end_date-owb_creation_date)*24,2) < 8 then 1 else 0 END below_8hrs_ind,
        case when round((etl_end_date-owb_creation_date)*24,2) between 8 and 12 then 1 else 0 END between_8hrs_n_12hrs_ind,
        case when round((etl_end_date-owb_creation_date)*24,2) > 12 then 1 else 0 END over_12hrs_ind
from monitor_dw.log_flows
order by datatoload_date desc

/*
 This view is intended for analysis (roll-ups, cube etc) over the log_flows
*/
create or replace view monitor_dw.v_analyze_log_flows_clean
as
select  owb_creation_date, 
        to_char(owb_creation_date, 'DAY') owb_Creation_DAY,
        to_char(owb_creation_date, 'MM') owb_Creation_MM,
        to_char(owb_creation_date, 'YYYY') owb_Creation_YYYY,
        datatoload_date, 
        to_char(datatoload_date, 'DAY') datatoload_date_DAY,
        to_char(datatoload_date, 'MM') datatoload_date_MONTH, 
        round((target_end_date - owb_creation_date)*24, 2) to_target_hrs, round((etl_end_date-owb_creation_date)*24,2) to_end_hrs,
        case when round((etl_end_date-owb_creation_date)*24,2) < 8 then 1 else 0 END below_8hrs_ind,
        case when round((etl_end_date-owb_creation_date)*24,2) between 8 and 12 then 1 else 0 END between_8hrs_n_12hrs_ind,
        case when round((etl_end_date-owb_creation_date)*24,2) > 12 then 1 else 0 END over_12hrs_ind
from monitor_dw.log_flows
where 
trunc(owb_creation_date) not in (
    '15-FEB-2012',
    '16-FEB-2012',
    '29-FEB-2012',
    '01-MAR-2012',
    '03-MAR-2012',
    '04-MAR-2012',
    '06-MAR-2012',
    '09-MAR-2012',
    '10-MAR-2012',
    '13-MAR-2012',
    '16-MAR-2012',
    '17-MAR-2012',
    '22-MAR-2012',
    '27-MAR-2012',
    '03-APR-2012',
    '06-APR-2012',
    '07-APR-2012',
    '08-APR-2012',
    '10-MAY-2012'        
)
order by datatoload_date desc


/*
  LEVEL0 duration per month in bands
*/
select owb_Creation_YYYY, 
    owb_Creation_MM, 
    sum(below_8hrs_ind), 
    sum( between_8hrs_n_12hrs_ind), 
    sum(over_12hrs_ind),
    tot_runs,     
    round((sum(below_8hrs_ind)/tot_runs) * 100, 2) pcnt_bel_8hrs,
    round((sum(between_8hrs_n_12hrs_ind)/tot_runs) * 100, 2) pcnt_be_8_12hrs,
    round((sum(over_12hrs_ind)/tot_runs) * 100, 2) pcnt_over_12hrs
from (select count(*) over() tot_runs, t.* from monitor_dw.v_analyze_log_flows_clean t) 
group by tot_runs, owb_Creation_YYYY, owb_Creation_MM
order by owb_Creation_YYYY, owb_Creation_MM


/*
Alternative for level0 from monitor_dw.log_flows (for up to target_dw loading)
*/
select 
    case when to_target_hrs < 8 then 'lt_8'
            when to_target_hrs between 8 and 11  then 'bt_8_12'
            when to_target_hrs between 13 and 17  then 'bt_13_17'
            else  'gt_17'
    end time_band_totarget,
    count(*) frequency            
from (
select  datatoload_date, round((target_end_date - owb_creation_date)*24, 2) to_target_hrs, round((etl_end_date-owb_creation_date)*24,2) to_end_hrs
from monitor_dw.log_flows
order by datatoload_date desc
)
group by
    case when to_target_hrs < 8 then 'lt_8'
            when to_target_hrs between 8 and 11  then 'bt_8_12'
            when to_target_hrs between 13 and 17  then 'bt_13_17'
            else  'gt_17'
    end
order by frequency desc 

/*
Alternative for level0 from monitor_dw.log_flows (for up to the end of level0)
*/
select 
    case when to_end_hrs < 8 then 'lt_8'
            when to_end_hrs between 8 and 11  then 'bt_8_12'
            when to_end_hrs between 13 and 17  then 'bt_13_17'
            else  'gt_17'
    end time_band_toend,
    count(*) frequency            
from (
select  datatoload_date, round((target_end_date - owb_creation_date)*24, 2) to_target_hrs, round((etl_end_date-owb_creation_date)*24,2) to_end_hrs
from monitor_dw.log_flows
order by datatoload_date desc
)
group by
    case when to_end_hrs < 8 then 'lt_8'
            when to_end_hrs between 8 and 11  then 'bt_8_12'
            when to_end_hrs between 13 and 17  then 'bt_13_17'
            else  'gt_17'
    end
order by frequency desc 



/*
Alternative with use of the workflow repository
How much time does it take to run the 'LEVEL0_DAILY', 'PER_MAIN','DAILY_USAGE_PF','USAGE_EXTRACTION_PF' flow in the last three months
*/
SELECT a.item_type, a.item_key, a.root_activity,
       a.root_activity_version, a.user_key, a.owner_role,
       a.parent_item_type, a.parent_item_key, a.parent_context,
       a.begin_date, a.end_date, a.ha_migration_flag,
       a.security_group_id
  FROM owf_mgr.wf_items a
  WHERE root_activity IN ('LEVEL0_DAILY', 'PER_MAIN','DAILY_USAGE_PF','USAGE_EXTRACTION_PF')
  AND TO_DATE(begin_date, 'DD/MM/YYYY') >= TO_DATE('1/3/2011', 'DD/MM/YYYY')
  ORDER BY ROOT_ACTIVITY, BEGIN_DATE



/*
Compute Average Duration--> How much time does it take to run the LEVEL0_DAILY flow in the last three months 
*/
select avg(duration),  sum(duration*freq)/sum(freq)
from (
    select EXECUTION_NAME, duration, count(*) as freq
    from (
    select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration  
    from owbsys.all_rt_audit_executions a 
    where a.EXECUTION_NAME like ('LEVEL0_DAILY')--, '%DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
    and   A.CREATED_ON>to_date('01/10/2011','dd/mm/yyyy') 
    and a.execution_audit_status='COMPLETE' and a.return_result='OK'
    -- and PARENT_AUDIT_EXECUTION_ID is null 
    ORDER BY A.CREATED_ON DESC
    )
    group by EXECUTION_NAME, duration
    order by 1, 2 asc
) t1
where
t1.freq > 2 -- exclude outliers

/*
How much time does it take to run the PERIF_MAIN flow in the last three months
*/
select duration, count(*)
from (
select  CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration  
from owbsys.all_rt_audit_executions a 
where a.EXECUTION_NAME='PERIF_MAIN' 
and   A.CREATED_ON>to_date('01/11/2010','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
-- and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by duration
order by 2 desc

/*
Compute avg(duration) --> How much time does it take to run the PERIF_MAIN flow in the last three months
*/
select avg(duration),   sum(duration*freq)/sum(freq)
from (
    select duration, count(*) as freq
    from (
    select  CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration  
    from owbsys.all_rt_audit_executions a 
    where a.EXECUTION_NAME= 'PER_MAIN' --'PERIF_MAIN' 
    and   A.CREATED_ON>to_date('01/11/2010','dd/mm/yyyy') 
    and a.execution_audit_status='COMPLETE' and a.return_result='OK'
    -- and PARENT_AUDIT_EXECUTION_ID is null 
    ORDER BY A.CREATED_ON DESC
    )
    group by duration
    order by 2 desc
) t1
where
t1.freq > 2 -- exclude outliers
and duration > 0

/*
How much time does it take to run the DAILY_USAGE_PF flow in the last three months: note DAILY_USAGE_PF does circles and so in some cases is is executed more than once
*/
select EXECUTION_NAME, duration, count(*)
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration  
from owbsys.all_rt_audit_executions a 
where a.EXECUTION_NAME like ('%DAILY_USAGE_PF')--, '%DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
and   A.CREATED_ON>to_date('01/11/2010','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
-- and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME, duration
order by 1, 2 asc

/*
Compute Average Duration--> How much time does it take to run the DAILY_USAGE_PF flow in the last three months : note DAILY_USAGE_PF does circles and so in some cases is is executed more than once
*/
select avg(duration), sum(duration*freq)/sum(freq)
from (
    select EXECUTION_NAME, duration, count(*) as freq
    from (
    select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration  
    from owbsys.all_rt_audit_executions a 
    where a.EXECUTION_NAME like ('%DAILY_USAGE_PF')--, '%DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
    and   A.CREATED_ON>to_date('01/11/2010','dd/mm/yyyy') 
    and a.execution_audit_status='COMPLETE' and a.return_result='OK'
    -- and PARENT_AUDIT_EXECUTION_ID is null 
    ORDER BY A.CREATED_ON DESC
    )
    group by EXECUTION_NAME, duration
    order by 1, 2 asc
) t1
where
t1.freq > 2 -- exclude outliers
and t1.duration > 0

/*
How much time does it take to run the USAGE_EXTRACTION_PF flow in the last three months (we want the net extraction time and not the DAILY_USAGE_PF time)
Note: that USAGE_EXTRACTION_PF might be doing circles, these are inevitably included in the following
*/
    /*
      Get the duration of the net extraction time from USAGE_EXTRACTION_PF
    */
    select EXECUTION_NAME 
           --,duration as hrs
           ,   case 
                            when  duration > 20 then -20 -- meaning greater than 20
                            else duration             
                        end as duration_time_band
           ,count(*) as frequency
           ,count(*) / max(tot_cnt) as prcnt 
    from (
        /*
        Get the net extraction time from USAGE_EXTRACTION_PF for the current year
        */
        select t2.EXECUTION_NAME, t2.CREATED_ON, t1.CREATED_ON, t2.UPDATED_ON,  trunc((t1.created_on - t2.created_on)*24) duration, count(*) over() as tot_cnt 
        from
        ( -- execution times of DAILY_USAGE_PF, invoked by USAGE_EXTRACTION_PF, in the current year
            select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
            from owbsys.all_rt_audit_executions a 
            where a.EXECUTION_NAME = 'USAGE_EXTRACTION_PF:DAILY_USAGE_PF'--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/11/2010','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            --and PARENT_AUDIT_EXECUTION_ID is null 
            ORDER BY A.CREATED_ON DESC
        ) t1 join
        ( -- execution times of USAGE_EXTRACTION_PF
            select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
            from owbsys.all_rt_audit_executions a 
            where a.EXECUTION_NAME in ('USAGE_EXTRACTION_PF')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/11/2010','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            --and PARENT_AUDIT_EXECUTION_ID is null 
            ORDER BY A.CREATED_ON DESC
        ) t2
        on (trunc(t1.updated_on,'MI') = trunc(t2.updated_on,'MI'))
    )
    group by EXECUTION_NAME
            --, duration
               ,case 
                            when  duration > 20 then -20
                            else duration             
                        end 
    order by 4 desc

/*
Compute Average Duration--> How much time does it take to run the USAGE_EXTRACTION_PF flow in the last three months (we want the net extraction time and not the DAILY_USAGE_PF time)
Note: that USAGE_EXTRACTION_PF might be doing circles, these are inevitably included in the following
*/
/*
  Get the average net extraction time for a single rate date from USAGE_EXTRACTION_PF
  Exclude the cases where it is run in catch up mode (i.e. running continously) - Assumption: < 11 hours
*/
select avg(duration_time_band), sum(duration_time_band*frequency)/sum(frequency) 
from (
    /*
      Get the duration of the net extraction time from USAGE_EXTRACTION_PF
    */
    select EXECUTION_NAME 
           --,duration as hrs
           ,   case 
                            when  duration > 15 then -15 -- meaning greater than 20
                            else duration             
                        end as duration_time_band
           ,count(*) as frequency
           ,count(*) / max(tot_cnt) as prcnt 
    from (
        /*
        Get the net extraction time from USAGE_EXTRACTION_PF for the current year
        */
        select t2.EXECUTION_NAME, t2.CREATED_ON, t1.CREATED_ON, t2.UPDATED_ON t2updated_on, t1.updated_on t1updated_on,  trunc((t1.created_on - t2.created_on)*24) duration, count(*) over() as tot_cnt 
        from
        ( -- execution times of DAILY_USAGE_PF, invoked by USAGE_EXTRACTION_PF, in the current year
            select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
            from owbsys.all_rt_audit_executions a 
            where a.EXECUTION_NAME = 'USAGE_EXTRACTION_PF:DAILY_USAGE_PF'--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/09/2011','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            --and PARENT_AUDIT_EXECUTION_ID is null 
            ORDER BY A.CREATED_ON DESC
        ) t1 join
        ( -- execution times of USAGE_EXTRACTION_PF
            select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
            from owbsys.all_rt_audit_executions a 
            where a.EXECUTION_NAME in ('USAGE_EXTRACTION_PF')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/09/2011','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            --and PARENT_AUDIT_EXECUTION_ID is null 
            ORDER BY A.CREATED_ON DESC
        ) t2
        on (trunc(t1.updated_on,'MI') = trunc(t2.updated_on,'MI'))
    )
    group by EXECUTION_NAME
            --, duration
               ,case 
                            when  duration > 15 then -15
                            else duration             
                        end 
    order by 4 desc
)
where
frequency > 2 -- exclude outliers and special cases
and duration_time_band > 0 --and duration_time_band <11


/* how much time does it take for a child flow to run */
select child_duration_overall_start, count(*)
from (
select  child.EXECUTION_NAME, child.CREATED_ON,  child.UPDATED_ON,  trunc((child.updated_on-child.created_on)*24) child_duration
        ,parent.CREATED_ON par_created_on,  parent.UPDATED_ON par_updated_on, trunc((parent.updated_on-parent.created_on)*24) parent_duration
        ,trunc((child.updated_on-parent.created_on)*24) child_duration_overall_start
from owbsys.all_rt_audit_executions child, owbsys.all_rt_audit_executions parent  
where child.EXECUTION_NAME like 'RUN_PDA%' --like 'LEVEL1_FCT_ORD_4TARGET%' 
and   child.CREATED_ON>to_date('01/12/2008','dd/mm/yyyy') 
and child.execution_audit_status='COMPLETE' and child.return_result='OK'
and child.TOP_LEVEL_EXECUTION_AUDIT_ID = parent.execution_audit_id
and parent.execution_audit_status='COMPLETE' and parent.return_result='OK'
-- and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY 1 DESC
)
group by child_duration_overall_start
order by 2 desc

/*
Monitor execution of flows, mappings or procedures from warehouse builder
Π.χ. πότε ξεκίνησαν και πότε σταμάτησαν και ποιός είναι ο γονικός flow, πόσο χρόνο έκαναν
*/

select *
from ALL_RT_AUDIT_EXECUTIONS
where 
task_name like 'LEVEL0_DAILY' -- '%PDA_JOB%'  '%TRUNCATE_PROCEDURE%'
order by created_on desc

/*
Monitor execution of mappings. Πότε ξεκίνησαν πότε σταμάτησαν πόσα records επεξεργάστηκαν, πόσο χρόνο έκαναν κλπ
(και αυτή και η owbsys.ALL_RT_AUDIT_PROC_RUN_ERRORS είναι Views που χρησιμοποιούν τον owbsys.wb_rt_audit
*/
select *
from ALL_RT_AUDIT_MAP_RUNS
where map_name like '%GET_%'
order by created_on desc

/*
Error messages in the execution of mappings 
*/
select *
from owbsys.ALL_RT_AUDIT_MAP_RUN_ERRORS a join owbsys.ALL_RT_AUDIT_MAP_RUNS b on a.map_run_id = b.map_run_id 
order by a.created_on desc

/*

??? Error messages in the execution of procedures 
*/
select *
from owbsys.ALL_RT_AUDIT_PROC_RUN_ERRORS a join owbsys.ALL_RT_AUDIT_MAP_RUNS b on a.map_run_id = b.map_run_id
order by a.created_on desc

/*
Παρακολούθηση των mappings (πίνακας όχι view)
*/
select *
from owbsys.wb_rt_audit
where RTA_LOB_NAME like '%ORDER_FCT_LOAD_STG%'
order by CREATION_DATE desc

/*
Παρακολούθηση των mappings (πίνακας όχι view)
exec_mode (αν είνσι setbased ή rowbased)
*/
select *
from owbsys.wb_rt_audit_detail
where RTD_NAME like '%GET%'
order by CREATION_DATE desc

/*
ϊδιο με το wb.ALL_RT_AUDIT_MAP_RUN_ERRORS απλά δεν είναι view
*/
select *
from owbsys.wb_rt_errors

/*
Γεμίζει με τα row που έκαναν fail. Δουλεύει μόνο για row based
*/
select *
from owbsys.wb_rt_error_sources


owbsys.wb_rt_audit_messages

owbsys.wb_rt_audit_message_lines


------------------------- DRAFT -------------------------

parent 4896808
top level 4888282

select *
from owbsys.all_rt_audit_executions a
where a.EXECUTION_AUDIT_ID in (4896808, 4888282) 
