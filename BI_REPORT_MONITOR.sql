/*

GOAL --> compute the total time needed to end OSM run (for BI Report)

*/


-- it returns the date that data have been loaded (pending orders of this date have been loaded)
select run_date from STAGE_DW.dw_control_table where procedure_name='OSM_LAST_RUN'


-- OSM_PKG.PRERUNCHECK
 function preRuncheck return NUMBER is
  lv_int_dist_dates NUMBER;
  lv_int_tables NUMBER;
  lv_run_osm NUMBER;
  lv_orders_to_load NUMBER;

  lv_run NUMBER;
  cannot_run exception;
  lv_min_int_date date;
  lv_ote_run_date date;
  lv_sbl_run_date date;
  lv_gns_run_date date;
  lv_osm_exception NUMBER;
  
  lv_wfm_run_date date;
  lv_osm_run_date date; 
  lv_soc_run_date date; 
  lv_per_run_date date; 
  lv_per_status varchar2(1);
  
  lv_dwh_interface_tabs_count NUMBER;
  
  begin
    
  
    loop 
    
         
            SELECT COUNT(*)
              INTO lv_dwh_interface_tabs_count
              FROM STAGE_DW.DW_INTERFACE_TABLES 
             WHERE FLOW_NAME = 'OSM_RUN'
               AND START_DATE_KEY <= (select run_date from STAGE_DW.dw_control_table where procedure_name='OSM_LAST_RUN');
         
        
          -- Check if OSM  can start
          select srcd.src_tbls ,  srcd.src_dts  ,    int.min_int_date , wfm.run_date , osm.run_date , soc.run_date, per.run_date, per.flow_status, orders.rows_to_load
          into   lv_int_tables, lv_int_dist_dates, lv_min_int_date,    lv_wfm_run_date, lv_osm_run_date, lv_soc_run_date, lv_per_run_date, lv_per_status, lv_orders_to_load
          from 
          (select count(distinct trunc(data_ref_date)) src_dts, count(*)  src_tbls from control_table@OSMPRD
          where upper(tablename) in
          ( SELECT UPPER(TABLE_NAME)
              FROM STAGE_DW.DW_INTERFACE_TABLES 
             WHERE FLOW_NAME = 'OSM_RUN'
               AND START_DATE_KEY <= (select run_date from STAGE_DW.dw_control_table where procedure_name='OSM_LAST_RUN')) ) srcd,
          (select min(data_ref_date-1) min_int_date from  CONTROL_TABLE@OSMPRD
            where upper(tablename) in 
              ( SELECT UPPER(TABLE_NAME)
                  FROM STAGE_DW.DW_INTERFACE_TABLES 
                 WHERE FLOW_NAME = 'OSM_RUN'
                   AND START_DATE_KEY <= (select run_date from STAGE_DW.dw_control_table where procedure_name='OSM_LAST_RUN')) ) int,
          (select run_date from STAGE_DW.dw_control_table where procedure_name='WFM_LAST_RUN') wfm, 
          (select run_date from STAGE_DW.dw_control_table where procedure_name='OSM_LAST_RUN') osm,
           (select run_date from STAGE_DW.dw_control_table where procedure_name='SOC_END_FORNMR_DATE') soc,
          (SELECT FLOW_STATUS, FLOW_BASEDATE-1 run_date   FROM STAGE_PERIF.FLOW_PROGRESS_STG  WHERE FLOW_NAME = 'ΠΕΡΙΦΕΡΕΙΕΣ') per  ,
           (select count(distinct ordernumber) rows_to_load from ordernumbers@osmprd where data_ref_date=
          (select  run_date+2 from STAGE_DW.dw_control_table where procedure_name='OSM_LAST_RUN') and status='RESOLVED') orders;
          
          if    lv_per_status='1' and 
                lv_osm_run_date < trunc(sysdate-1) and 
                lv_wfm_run_date > lv_osm_run_date and 
                lv_soc_run_date > lv_osm_run_date and 
                lv_per_run_date > lv_osm_run_date and
                lv_min_int_date > lv_osm_run_date and
                lv_int_dist_dates = 1
          then
              lv_run_osm := 1;
              if lv_orders_to_load < 500 then
                 lv_run_osm := 2;
              end if;   
          elsif  lv_osm_run_date >= trunc(sysdate-1)   then   
              lv_run_osm := -1;
          else 
              lv_run_osm := 0;
          end if;

  --        exit;
         
            
          exit when lv_run_osm <>0;
          dbms_lock.sleep(600);
          
    end loop;
    
    return lv_run_osm;
 
  exception when others then
             return -1;
  end;
  
  
  
/* Find what BI Report is waiting for */
SELECT srcd.src_tbls,
       srcd.src_dts,
       int.min_int_date,
       wfm.run_date wfm_run_date,
       osm.run_date osm_run_date,
       soc.run_date soc_run_date,
       per.run_date per_run_date,
       per.flow_status per_flow_status,
       orders.rows_to_load orders_rows_to_load
--  INTO lv_int_tables,
--       lv_int_dist_dates,
--       lv_min_int_date,
--       lv_wfm_run_date,
--       lv_osm_run_date,
--       lv_soc_run_date,
--       lv_per_run_date,
--       lv_per_status,
--       lv_orders_to_load
  FROM (SELECT COUNT (DISTINCT TRUNC (data_ref_date)) src_dts,
               COUNT (*) src_tbls
          FROM control_table@OSMPRD
         WHERE UPPER (tablename) IN
                  (SELECT UPPER (TABLE_NAME)
                     FROM STAGE_DW.DW_INTERFACE_TABLES
                    WHERE     FLOW_NAME = 'OSM_RUN'
                          AND START_DATE_KEY <=
                                 (SELECT run_date
                                    FROM STAGE_DW.dw_control_table
                                   WHERE procedure_name = 'OSM_LAST_RUN'))) srcd,
       (SELECT MIN (data_ref_date - 1) min_int_date
          FROM CONTROL_TABLE@OSMPRD
         WHERE UPPER (tablename) IN
                  (SELECT UPPER (TABLE_NAME)
                     FROM STAGE_DW.DW_INTERFACE_TABLES
                    WHERE     FLOW_NAME = 'OSM_RUN'
                          AND START_DATE_KEY <=
                                 (SELECT run_date
                                    FROM STAGE_DW.dw_control_table
                                   WHERE procedure_name = 'OSM_LAST_RUN'))) int,
       (SELECT run_date
          FROM STAGE_DW.dw_control_table
         WHERE procedure_name = 'WFM_LAST_RUN') wfm,
       (SELECT run_date
          FROM STAGE_DW.dw_control_table
         WHERE procedure_name = 'OSM_LAST_RUN') osm,
       (SELECT run_date
          FROM STAGE_DW.dw_control_table
         WHERE procedure_name = 'SOC_END_FORNMR_DATE') soc,
       (SELECT FLOW_STATUS, FLOW_BASEDATE - 1 run_date
          FROM STAGE_PERIF.FLOW_PROGRESS_STG
         WHERE FLOW_NAME = 'ΠΕΡΙΦΕΡΕΙΕΣ') per,
       (SELECT COUNT (DISTINCT ordernumber) rows_to_load
          FROM ordernumbers@osmprd
         WHERE     data_ref_date = (SELECT run_date + 2
                                      FROM STAGE_DW.dw_control_table
                                     WHERE procedure_name = 'OSM_LAST_RUN')
               AND status = 'RESOLVED') orders

/*
    Με πόσα pending orders βγήκε από το loop αναμονής η OSM_RUN?
*/

select *    
from STAGE_DW.OSM_PRERUNCHECK_EXITS
order by exit_datetime desc

               
/**** Calculate the total time for executing the BI Report  ***/

select execution_name, creation_date, last_update_date
from owbsys.wb_rt_audit_executions
where execution_name in (
'WFM_FL:MARK_FLOW_END'-- ενημέρωση ημερομηνίας ολοκήρωσης WFM
,'SOC_FCTS_TRG:SOC_FLOWPROGR_ENDSOC_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης SOC για NMR
,'PER_MAIN:FLOW_PROGRESS_LOAD_STG' -- ενημέρωση ημερομηνίας ολοκήρωσης Περιφερειών
,'OSM_RUN:PRERUNCHECK' -- έλεγχος έναρξης/λήξης OSM_RUN
,'OSM_RUN:OSM_1PRE' -- εκκίνηση φόρτωσης OSM_RUN
,'LEVEL1_DIM_4SHADOW:PERIF_FLOWPROGR_ENDSHADOW_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης shadow στη LEVEL0_DAILY
)
and creation_date > sysdate - 10
order by last_update_date desc

/*
trunc(soc_for_nmr.start_date) = trunc(dims_sha.start_date)
    AND  trunc(dims_sha.start_date) = trunc(per_main.start_date)
    AND trunc(per_main.start_date) = trunc(wfm_run.start_date)
    AND trunc(wfm_run.start_date) = trunc(osm_run.start_date)
*/


-- Query with dependent flow individual times
SELECT trunc(osm_run.start_date) DAY_OF_EXECUTION,
       dims_sha.duration_hrs_total + greatest(soc_for_nmr.duration_hrs_total,per_main.duration_hrs_total,wfm_run.duration_hrs_total) + osm_run.duration_hrs_total critical_path_hrs_total,
       dims_sha.duration_real_hrs + greatest(soc_for_nmr.duration_real_hrs,per_main.duration_real_hrs,wfm_run.duration_real_hrs) + osm_run.duration_real_hrs critical_path_real_hrs,
       CASE WHEN greatest(soc_for_nmr.duration_real_hrs,per_main.duration_real_hrs,wfm_run.duration_real_hrs) = soc_for_nmr.duration_real_hrs
                THEN 'SOC_FOR_NMR'
            WHEN greatest(soc_for_nmr.duration_real_hrs,per_main.duration_real_hrs,wfm_run.duration_real_hrs) = per_main.duration_real_hrs
                THEN 'PER_MAIN'
            WHEN greatest(soc_for_nmr.duration_real_hrs,per_main.duration_real_hrs,wfm_run.duration_real_hrs) = wfm_run.duration_real_hrs
                THEN 'WFM_RUN'
            ELSE null                
       END MOST_WAITED_REAL,       
       CASE WHEN greatest(soc_for_nmr.duration_hrs_total,per_main.duration_hrs_total,wfm_run.duration_hrs_total) = soc_for_nmr.duration_hrs_total
                THEN 'SOC_FOR_NMR'
            WHEN greatest(soc_for_nmr.duration_hrs_total,per_main.duration_hrs_total,wfm_run.duration_hrs_total) = per_main.duration_hrs_total
                THEN 'PER_MAIN'
            WHEN greatest(soc_for_nmr.duration_hrs_total,per_main.duration_hrs_total,wfm_run.duration_hrs_total) = wfm_run.duration_hrs_total
                THEN 'WFM_RUN'
            ELSE null                
       END MOST_WAITED_TOTAL,              
       osm_run.start_date osm_run_start_date,
       osm_run.end_date osm_run_end_date, 
       osm_run.duration_hrs_total osm_run_duration_hrs_total,
       osm_run.duration_real_hrs    osm_run_duration_real_hrs,
       dims_sha.start_date dims_sha_start_date,
       dims_sha.end_date dims_sha_end_date, 
       dims_sha.duration_hrs_total dims_sha_duration_hrs_total,
       dims_sha.duration_real_hrs    dims_sha_duration_real_hrs,
       per_main.start_date per_main_start_date,
       per_main.end_date per_main_end_date, 
       per_main.duration_hrs_total per_main_duration_hrs_total,
       per_main.duration_real_hrs    per_main_duration_real_hrs,
       soc_for_nmr.start_date soc_for_nmr_start_date,
       soc_for_nmr.end_date soc_for_nmr_end_date, 
       soc_for_nmr.duration_hrs_total soc_for_nmr_duration_hrs_total,
       soc_for_nmr.duration_real_hrs    soc_for_nmr_duration_real_hrs,      
       wfm_run.start_date wfm_run_start_date,
       wfm_run.end_date wfm_run_end_date, 
       wfm_run.duration_hrs_total wfm_run_duration_hrs_total,
       wfm_run.duration_real_hrs    wfm_run_duration_real_hrs                                                                       
FROM
(
    -- the time needed for'SOC_FCTS_TRG:SOC_FLOWPROGR_ENDSOC_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης SOC για NMR
    select root_created_on start_date,
           updated_on end_date,
           ROUND ( (updated_on - root_created_on) * 24 , 1) duration_hrs_total,
           ROUND (  (updated_on - root_created_on
                     - (  select updated_on - created_on 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = t.ROOT_EXECUTION_AUDIT_ID
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                        
                                            )
                      - (   select updated_on - created_on 
                            from owbsys.all_rt_audit_executions tt 
                            where
                                tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                                AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = t.ROOT_EXECUTION_AUDIT_ID
                                AND execution_audit_status = 'COMPLETE'
                                AND return_result = 'OK'                                                                                                            
                      )        
                    ) * 24 , 1)
           duration_real_hrs,
           'SOC for NMR' flow_name
    from (
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
                                                    AND flow_name = 'SOC_DW_MAIN'--nvl('&&flow_name',flow_name)
                                                    AND critical_ind = 1 --nvl('&&critical_ind',critical_ind)
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
           execution_name like '%SOC_FLOWPROGR_ENDSOC_ETL' -- trim('%&node_name')
           AND CONNECT_BY_ISLEAF = 1 
           AND execution_audit_status = 'COMPLETE'
           AND return_result = 'OK'  
           --------AND CONNECT_BY_ROOT execution_name = b.flow_name
        START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
        CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
         --ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
        ) t
        order by end_date desc
) soc_for_nmr,           
(
    -- the time needed for dims to be loaded into shadow
    select root_created_on start_date,
           updated_on end_date,
           ROUND ( (updated_on - root_created_on) * 24 , 1) duration_hrs_total,
           ROUND ( (updated_on - root_created_on) * 24 , 1) duration_real_hrs,
           'Load Dims into Shadow' flow_name
    from (
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
                                                AND flow_name = 'LEVEL0_DAILY'--nvl('&&flow_name',flow_name)
                                                AND critical_ind = 1 --nvl('&&critical_ind',critical_ind)
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
       execution_name like '%PERIF_FLOWPROGR_ENDSHADOW_ETL' -- trim('%&node_name')
       AND CONNECT_BY_ISLEAF = 1 
       --AND execution_audit_status = 'COMPLETE'
       --AND return_result = 'OK'  
       --------AND CONNECT_BY_ROOT execution_name = b.flow_name
    START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
     --ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
    ) t
    order by end_date desc           
) dims_sha,
(
    -- the time needed for PER_MAIN
    select created_on start_date,
           updated_on end_date,
           duration_hrs_total,
           duration_real_hrs,
           execution_name flow_name
    from (
    select 
      created_on,
       updated_on,
       ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
       round( ( updated_on - created_on
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
      duration_real_hrs, -- afairoume tous xronous anamonhs                         
       execution_name,
       b.critical_ind
    from owbsys.all_rt_audit_executions a, monitor_dw.dwp_etl_flows b
    where
        a.execution_name = flow_name
        AND flow_name = 'PER_MAIN' --nvl('&&flow_name',flow_name)
        AND critical_ind = 1 --nvl('&&critical_ind',critical_ind)
        and created_on > sysdate - &&days_back
        AND execution_audit_status = 'COMPLETE'
        AND return_result = 'OK'
    --order by  execution_name, CREATED_ON desc
    ) t2 
    order by end_date desc
)per_main,
(
    -- the time needed for WFM
    select created_on start_date,
           updated_on end_date,
           duration_hrs_total,
           duration_real_hrs,
           execution_name flow_name
    from (
    select 
      created_on,
       updated_on,
       ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
        round( ( updated_on - created_on
                                          - (select sum(updated_on - created_on) 
                                          from owbsys.all_rt_audit_executions tt 
                                          where
                                              tt.execution_name like 'WFM_RUN:PRERUNCHECK'
                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                              AND execution_audit_status = 'COMPLETE'
                                              AND return_result = 'OK'                                  
                                              )                                      
                                      )*24  -- hours
                              ,1) duration_real_hrs, -- afairoume tous xronous anamonhs                         
       execution_name,
       b.critical_ind
    from owbsys.all_rt_audit_executions a, monitor_dw.dwp_etl_flows b
    where
        a.execution_name = flow_name
        AND flow_name = 'WFM_RUN' --nvl('&&flow_name',flow_name)
        AND critical_ind = 1 --nvl('&&critical_ind',critical_ind)
        and created_on > sysdate - &&days_back
        AND execution_audit_status = 'COMPLETE'
        AND return_result = 'OK'
    --order by  execution_name, CREATED_ON desc
    ) t 
    order by end_date desc
)wfm_run,
(
    -- the time needed for OSM_RUN
    select created_on start_date,
           updated_on end_date,
           duration_hrs_total,
           duration_real_hrs,
           execution_name flow_name
    from (
    select 
      created_on,
       updated_on,
       ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
        round( ( updated_on - created_on
                                          - (select sum(updated_on - created_on) 
                                          from owbsys.all_rt_audit_executions tt 
                                          where
                                              tt.execution_name like 'OSM_RUN:PRERUNCHECK'
                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                              AND execution_audit_status = 'COMPLETE'
                                              AND return_result = 'OK'                                  
                                              )                                      
                                      )*24  -- hours
                              ,1)  duration_real_hrs, -- afairoume tous xronous anamonhs                         
       execution_name,
       b.critical_ind
    from owbsys.all_rt_audit_executions a, monitor_dw.dwp_etl_flows b
    where
        a.execution_name = flow_name
        AND flow_name = 'OSM_RUN' --nvl('&&flow_name',flow_name)
        AND critical_ind = 1 --nvl('&&critical_ind',critical_ind)
        and created_on > sysdate - &&days_back
        AND execution_audit_status = 'COMPLETE'
        AND return_result = 'OK'
    --order by  execution_name, CREATED_ON desc
    ) t 
    order by end_date desc
) osm_run
WHERE
    trunc(soc_for_nmr.start_date) = trunc(dims_sha.start_date)
    AND  trunc(dims_sha.start_date) = trunc(per_main.start_date)
    AND trunc(per_main.start_date) = trunc(wfm_run.start_date)
    AND trunc(wfm_run.start_date) = trunc(osm_run.start_date)
order by osm_run.end_date desc       

-- tune the above query
execute dbms_sqltune.accept_sql_profile(task_name =>'91pqs98qdc8jg_bireport', task_owner => 'NKARAG', replace =>TRUE, force_match=>TRUE);