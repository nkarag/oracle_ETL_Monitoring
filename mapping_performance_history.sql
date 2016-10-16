
/* Find execution history of the top N heaviest mappings of the last run for a specific flow */ 
select 
        HIST.TOP_LEVEL_EXECUTION_AUDIT_ID,
        hist.execution_audit_id,
        --hist.execution_name,
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
                                             select execution_audit_id
                                             from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                             where 
                                                execution_name = flow_name
                                                and PARENT_EXECUTION_AUDIT_ID IS NULL
                                                AND flow_name = nvl('&&flow_name',flow_name)
                                                AND critical_ind = nvl('&&critical_ind',critical_ind)
                                                -- restrictions for the main flow
                                                and created_on > sysdate - 1
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                        
                                            )
        -- restricitons for all the nodes (not just the root)    
        AND CREATED_ON > SYSDATE - 1
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
    and hist.created_on > sysdate - &&days_back 
order by
    top.ranking asc, hist.execution_name, hist.created_on desc;    
    
    