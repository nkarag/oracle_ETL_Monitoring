
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

-- check if a montlhy flow is running
select  CASE    WHEN   (select run_date from stage_dw.dw_control_table where procedure_name = 'LAST_RUN_END_TIME') < sysdate  -- LEVEL0 is not running 
                    AND last_day((select run_date from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')) = (select run_date /*date'2015-08-31'*/ from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN') 
                  THEN    1
                WHEN    (select run_date from stage_dw.dw_control_table where procedure_name = 'LAST_RUN_END_TIME') > sysdate -- LEVEL0 is running
                    AND last_day((select run_date+1 from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')) = (select run_date+1 /*date'2015-08-31'*/ from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')
                  THEN 1
                ELSE 0
        END monthly_flow_ind
from dual  


/*************************
   What NMR (KPIDW_MAIN) is waiting for
**************************/
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


-- check if CTO has finished for today
select DWADMIN.CONC_STATS_COLLECTION.CTO_HAS_FINISHED from dual;

/*****************************
-- What CTO is waiting for
*****************************/
with
level0 as
(
    -- DAILY DWH    
    SELECT RUN_DATE + 1 temp_level0_dw
    FROM STAGE_DW.DW_CONTROL_TABLE      
    WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN' 
),
genesis as
(
    -- GENESYS
    SELECT RUN_DATE + 1 temp_genesys_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'GENESYS_LAST_RUN'
),
wfm as
(                 
    -- WFM
    SELECT RUN_DATE + 1 temp_wfm_date
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'WFM_LAST_RUN'
),
per_main as
(
    -- ΠΕΡΙΦΕΡΕΙΕΣ    
    SELECT FLOW_BASEDATE temp_per_date    
    FROM STAGE_PERIF.FLOW_PROGRESS_STG      
    WHERE FLOW_NAME = 'PERIF_PRESENT_AREA_END'      
),
faults as
(
    -- FAULTS
    SELECT RUN_DATE + 1 temp_faults_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'FAULT_LAST_RUN'
),         
soc4nmr as
(
    -- SIEBEL    
    SELECT RUN_DATE + 1 temp_soc4nmr_date      
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE'
),       
cto as
(                 
    -- CTO KPI    
    SELECT RUN_DATE + 1 temp_date_cto
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'CTO_LAST_RUN'
)         
select
    temp_date_cto CTO_TO_BE_LOADED,
    -- WFM subflow: WFM KPIs και Genesis
    temp_level0_dw,
    temp_wfm_date,
    temp_genesys_date,
    CASE WHEN 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_wfm_date OR
            temp_date_cto >= temp_genesys_date
        THEN    'WAITING'
        ELSE    'OK'        
    END  WFM_SUBFLOW_STATUS,              
    -- FAULTS subflow: FAULTS KPIs Siebel Faults και Προμηθέα (LL, καλωδιακές)
    temp_per_date,
    temp_level0_dw,
    temp_faults_date,
    CASE WHEN 
            temp_date_cto >= temp_per_date OR 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_faults_date         
        THEN    'WAITING'
        ELSE    'OK'        
    END  FAULTS_SUBFLOW_STATUS,                     
    -- ORDERS subflow: ORDER KPIs από Siebel, Woms, Προμηθέα
    temp_per_date,
    temp_level0_dw,
    temp_soc4nmr_date,
    temp_faults_date,
    CASE WHEN 
            temp_date_cto >= temp_per_date OR 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_soc4nmr_date  OR
            temp_date_cto >= temp_faults_date         
        THEN    'WAITING'
        ELSE    'OK'        
    END  ORDERS_SUBFLOW_STATUS
from level0, genesis, wfm, per_main, faults, soc4nmr, cto;

/*************************************************
    Check what PEND_ORD_RUN is waiting for    
    
    NOTE
    Για το COM (Order Status) υπάρχουν δύο έλεγχοι,
    ένας με βάση το control table του Interface (COM_CONTROL_TABLE)
    και άλλος ένας με βάση τον αριθμό των orders που έχουν γίνει
    processed από το COM (COM_PROCESSED_ORDERS). Αν ο πρώτος έλεγχος
    είναι Err και ο δεύτερος είναι OK (δηλαδή το COM έχει επεξεργαστεί
    το μεγαλύτερο ποσοστό των orders) η ροή προχωράει
**************************************************/

WITH cntrl AS
  ( SELECT DISTINCT PROM.MIN_DATA_REF_DATE lv_prom_date,
    SIEBEL.MIN_DATA_REF_DATE lv_sbl_date,
    UCM.MIN_DATA_REF_DATE lv_ucm_date,
    BUS.MIN_DATA_REF_DATE lv_bus_date,
    WFM.MIN_DATA_REF_DATE lv_wfm_date,
    WFM.INT_PINAKES lv_wfm_cnt,
    PROM.INT_PINAKES lv_prom_cnt,
    SIEBEL.INT_PINAKES lv_sbl_cnt,
    UCM.INT_PINAKES lv_ucm_cnt,
    BUS.INT_PINAKES lv_bus_cnt ,
    orders.resolved lv_resolved,
    orders.pending lv_pending,
    flow.run_date lv_flow_date
  FROM
    -- Promitheas interface (5 tables)
    (
    SELECT DISTINCT MIN(DATA_REF_DATE) MIN_DATA_REF_DATE,
      COUNT(*) INT_PINAKES
    FROM CONTROL_TABLE@PROMITHEAS
    WHERE PINAKAS IN ('DWH40','DWH41','DWH42','DWH43','DWH45')
    ) PROM,
    -- Siebel Interface (8 tables)
    (
    SELECT DISTINCT MIN(DATA_REF_DATE) MIN_DATA_REF_DATE,
      COUNT(*) INT_PINAKES
    FROM dwh_CONTROL_TABLE@siebelprod
    WHERE table_name IN ('DWH_ADDR_PER','DWH_CONTACT','DWH_ORDER','DWH_ORDER_ITEM','DWH_PROD_INT','DWH_LOV', 'DWH_CC_USER','DWH_SHIP_DETAIL')
    ) SIEBEL,
    -- UCM interface (2 tables)
    (
    SELECT DISTINCT MIN(DATA_REF_DATE) MIN_DATA_REF_DATE,
      COUNT(*) INT_PINAKES
    FROM CONTROL_TABLE@UCM
    WHERE table_name IN ('UCM_CUSTOMERS','UCM_CROSS_REFERENCES')
    ) UCM,
    -- WFM interface 1 table
   (
    SELECT NVL(
      ( SELECT DISTINCT MIN(DATA_REF_DATE) MIN_DATA_REF_DATE
      FROM DWH_CONTROL_TABLE@WFM
      WHERE table_name IN ('DWH_SLOTS_AVAIL_LOG')
      AND COMPLETION_FLG='Y'
      ), to_date('1/1/2000', 'dd/mm/yyyy')) MIN_DATA_REF_DATE,
      NVL(
      (SELECT COUNT(*) INT_PINAKES
      FROM DWH_CONTROL_TABLE@WFM
      WHERE table_name IN ('DWH_SLOTS_AVAIL_LOG')
      AND COMPLETION_FLG='Y'
      ), 0) INT_PINAKES
    FROM dual
    ) WFM,
    -- BUS interface, 6 tables
    (
    SELECT MIN(data_ref_date-1) OVER (PARTITION BY 1) MIN_DATA_REF_DATE,
      COUNT(                *) OVER (PARTITION BY 1) INT_PINAKES
    FROM CONTROL_TABLE@OSMPRD
    WHERE upper(tablename) IN
      (SELECT UPPER(TABLE_NAME)
      FROM STAGE_DW.DW_INTERFACE_TABLES
      WHERE FLOW_NAME     = 'OSM_RUN'
      AND START_DATE_KEY <=
        (SELECT run_date
        FROM STAGE_DW.dw_control_table
        WHERE procedure_name='ORDPEND_LAST_RUN'
        )
      )
    ) BUS,
    (SELECT SUM(
      CASE
        WHEN status='RESOLVED'
        THEN 1
        ELSE 0
      END) resolved,
      SUM(
      CASE
        WHEN status='PENDING'
        THEN 1
        ELSE 0
      END) pending
    FROM
      (SELECT ordernumber,
        status,
        row_number() over (partition BY ordernumber order by
        CASE
          WHEN status='RESOLVED'
          THEN 0
          ELSE 1
        END) rn
      FROM ordernumbers@osmprd
      WHERE data_ref_date=
        (SELECT run_date+2
        FROM STAGE_DW.dw_control_table
        WHERE procedure_name='ORDPEND_LAST_RUN'
        )
      )
    WHERE rn=1
    ) orders,
    (SELECT run_date
    FROM stage_dw.dw_control_table
    WHERE procedure_name='ORDPEND_LAST_RUN'
    ) flow
  )
SELECT
  CASE
    WHEN lv_prom_date > lv_flow_date
    AND lv_prom_cnt   = 5
    THEN 'OK'
    ELSE 'Err'
  END PROM,
  CASE
    WHEN lv_sbl_date > lv_flow_date
    AND lv_sbl_cnt   = 8
    THEN 'OK'
    ELSE 'Err'
  END SBL,
  CASE
    WHEN lv_ucm_date > lv_flow_date
    AND lv_ucm_cnt   = 2
    THEN 'OK'
    ELSE 'Err'
  END UCM,
  CASE
    WHEN lv_bus_date > lv_flow_date
    AND lv_bus_cnt   = 6
    THEN 'OK'
    ELSE 'Err'
  END COM_Control_Table,
  CASE
    WHEN lv_bus_cnt                           = 6
    AND lv_bus_date                          <= lv_flow_date
    AND NVL(lv_resolved,0)+NVL(lv_pending,0) >= 50000
    AND lv_pending        *100/lv_resolved   <= 10
    THEN 'OK'
    ELSE 'Err'
  END COM_Processed_Orders,
  CASE
    WHEN lv_wfm_date > lv_flow_date
    AND lv_wfm_cnt   = 1
    THEN 'OK'
    ELSE 'Err'
  END wfm
FROM cntrl ;



/*
    ***OLD VERSION***
 Check what CTO_MAIN is waiting for
 
 Note:
    All dates below denote data ref dates "to be loaded" or that are currently loading
*/

with 
per_main as 
(
-- ΠΕΡΙΦΕΡΕΙΕΣ    
    SELECT FLOW_BASEDATE  dt  
        --INTO temp_date    
        FROM STAGE_PERIF.FLOW_PROGRESS_STG      
        WHERE FLOW_NAME = 'PERIF_PRESENT_AREA_END'  
),
level0 as
(
-- DAILY DWH    
    SELECT (RUN_DATE + 1) dt -- INTO temp_date_dw     
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN'
),
soc4nmr as
(
-- SOC for NMR
    SELECT (RUN_DATE + 1) dt 
      --INTO temp_siebel_date      
      FROM STAGE_DW.DW_CONTROL_TABLE        
      WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE' 
),
cmp as
(
   -- CAMAPINGS-FAULTS
    SELECT (RUN_DATE + 1) dt
        --INTO temp_crm_date    
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'CRM_LAST_RUN' 
),
gns as
(
-- GENESYS
  SELECT (RUN_DATE + 1) dt  
    --INTO temp_genesys_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'GENESYS_LAST_RUN'
),    
wfm as
(
-- wfm
SELECT (RUN_DATE + 1) dt
        --INTO temp_wfm_date    
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'WFM_LAST_RUN'
),
cto as
(
 -- CTO KPI    
    SELECT (RUN_DATE + 1) dt --INTO temp_date_cto
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'CTO_LAST_RUN'
)
select
    cto.dt CTO_to_be_loaded,
    case when cto.dt >= per_main.dt then 'WAITING' else 'OK' end PER_MAIN_STATUS,
    per_main.dt PERIF_PRESENT_AREA_END,
    case when cto.dt >= level0.dt then 'WAITING' else 'OK' end LEVEL0_STATUS,
    level0.dt level0_date_plus1,
    case when cto.dt >= soc4nmr.dt then 'WAITING' else 'OK' end SOC4NMR_STATUS,
    soc4nmr.dt soc4nmr,
    case when cto.dt >= cmp.dt then 'WAITING' else 'OK' end CMP_STATUS,
    cmp.dt cmp,
    case when cto.dt >= gns.dt then 'WAITING' else 'OK' end GENESYS_STATUS,
    gns.dt genesys,   
    case when cto.dt >= wfm.dt then 'WAITING' else 'OK' end WFM_STATUS,
    wfm.dt wfm
from  per_main, level0,  soc4nmr, cmp, gns, wfm, cto;

/*
    Find all major flows executing NOW
*/
select /*+ parallel(a 32) */ *
from owbsys.all_rt_audit_executions a
where A.PARENT_EXECUTION_AUDIT_ID is NULL
AND A.EXECUTION_AUDIT_STATUS = 'BUSY'



--Πόσα Updates έγιναν ανά ημέρα (log_date) στα order headers 
select*   from STAGE_DW.SOCPATCH_LOG where check_name = 'OH STAGE UPDATE STATUS' order by log_date

--Πόσα Updates έγιναν ανά ημέρα (log_date) στα order lines 
select*   from STAGE_DW.SOCPATCH_LOG where check_name = 'OL STAGE UPDATE STATUS' order by log_date


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

alter session set nls_date_format = 'dd-mm-yyyy'


/***************
  Query to find the execution history of a specific node (leaf node or subflow) and remaining time until the end of main flow or other node
  
  The query returns the duration in minutes of the execution of the specific node in the history of interest.
  Also, it returns the minutes until the end of the Main Flow and the minutes until the end 
    of another node-milestone ( -- check script "NMR/CTO Flows Dependencies" next to get all the milestones of interest
    
                                   e.g.SOC_FLOWPROGR_ENDSOC_ETL -- ενημέρωση ημερομηνίας ολοκλήρωσης SOC για NMR
                                  FLOW_PROGRESS_NMREND_LOAD_STG  -- ενημέρωση ημερομηνίας 'PERIF_NMR_END' από PER_MAIN για NMR
                                , PERIF_FLOWPROGR_ENDSHADOW_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης shadow στη LEVEL0_DAILY
                                ,LEVEL1_FINALIZE - αναμονή της KPI_DW στη LEVEL0_DAILY
                                ,FLOW_PRESENT_PROGRESS_LOAD_STG - κόμβος στην PER_MAIN όπου ενημερώνεται η presentation area FLOW_NAME = 'PERIF_PRESENT_AREA_END'
                                                                - το σημείο αυτό το περιμένει τόσο η SOC_DW_MAIN, όσο και η WFM_RUN)
                                 Also by checking e.g. the UPDATED_ON column for node PRERUNCHECK of SOC_DW_MAIN you can see the times
                                 that the Siebel interface was loaded.
                                 Also by checking e.g. the UPDATED_ON column for node PRESTAGE_CHECKPROM_PROC of PER_MAIN you can see the times
                                 that the Promitheus interface was loaded.
                                 ,FLOW_DW_PISTAGE_LOAD_STG   - Κόμβος στην LEVEL0_DAILY που δηλώνει το πέρας της ενημέρωσης της περιοχής STAGE που είναι απαραίτητο για τη φόρτωση του MC customer
                                 
  The query also distinguishes between duration of the whole node and duration of the mapping only (within this node) (see column MINS_MAP_ONLY_ALL_TARGETS).
  This is for the cases where a pre and/or post mapping procedure is invoked and so in the overall duration of the node the pre/post procedure time is 
  also embedded.
  Finally, for nodes with multiple targets it shows the individual time per target table (MINS_MAP_ONLY_SINLGE_TARGET)                                                                                                                              
  
  Parameters:
  flow_name    (optional) The Main flow name e.g. SOC_DW_MAIN
  critical_ind  (optional)  0/1 if the main flow is critical
  days_back               Num of days back from sysdate of required history
  from_date            (optional)    Use this date if you dont want to use sysdate as the date of reference for going '&days_back' in history
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
                                            --and created_on > sysdate - &&days_back
                                            and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                                            --AND execution_audit_status = 'COMPLETE'
                                            --AND return_result = 'OK'                                                        
                                        )
    -- restricitons for all the nodes (not just the root)    
    --AND CREATED_ON > SYSDATE - (&days_back)
    and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1    
     AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
     AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
   --AND a.execution_audit_status = 'COMPLETE'
   --AND a.return_result = 'OK'
),
--q2 as 
--( -- query to get mapping  execution details
--    select /*+ materialize */ MAP_NAME, MAP_TYPE, START_TIME, END_TIME, round(ELAPSE_TIME/60,1) elapsed_time_mins, RUN_STATUS, SOURCE_LIST, TARGET_LIST, NUMBER_ERRORS, NUMBER_RECORDS_SELECTED, NUMBER_RECORDS_INSERTED, NUMBER_RECORDS_UPDATED, NUMBER_RECORDS_DELETED, NUMBER_RECORDS_MERGED 
--    from OWBSYS.ALL_RT_AUDIT_MAP_RUNS 
--    where 1=1
--     --start_time > SYSDATE - (&&days_back)
--     and start_time between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1    
--     AND to_char(start_time, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(start_time, 'DD') end    
--     AND trim(to_char(start_time, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(start_time, 'DAY')) end
--),
q3 as
( -- query to get from AUDIT_DETAILS the exetuion time of the mapping (i.e., not including pre/post session procedures within the node)
    select /*+ materialize leading(t2 t1) */ T2.RTE_ID, --T3.EXECUTION_NAME, T3.CREATED_ON, T3.UPDATED_ON, 
            round(T1.RTD_ELAPSE/60,1) MINS_MAP_ONLY_SINGLE_TARGET, 
            round(T2.RTA_ELAPSE/60,1) MINS_MAP_ONLY_ALL_TARGETS,
            min(T1.CREATION_DATE) over(partition by T2.RTE_ID ) mapping_created_on_min, 
            max(T1.LAST_UPDATE_DATE) over(partition by T2.RTE_ID) mapping_updated_on_max,
            --ROUND ( (t3.updated_on - t3.created_on) * 24 * 60, 1) - round(T1.RTD_ELAPSE/60,1) MINS_IN_PRE_POST_MAPPING,
            T2.RTA_PRIMARY_SOURCE,
            T1.RTD_TARGET,
            T1.RTD_NAME,
            T1.RTD_SELECT, T1.RTD_INSERT, T1.RTD_UPDATE, T1.RTD_DELETE, T1.RTD_MERGE       
    from OWBSYS.OWB$WB_RT_AUDIT_DETAIL t1, OWBSYS.OWB$WB_RT_AUDIT t2--, q1 t3
    where 1=1
        AND T1.RTA_IID = T2.RTA_IID
        -- impose the time constraint also in here so as to limit the number of rows
        and T2.RTA_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1    
        AND to_char(T2.RTA_DATE, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(T2.RTA_DATE, 'DD') end    
        AND trim(to_char(T2.RTA_DATE, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(T2.RTA_DATE, 'DAY')) end       
        --AND t3.execution_name like trim('%&&node_name')
        --AND T2.RTA_LOB_NAME like '%SOC_SAVEDESK_FCT_UPD%'
        --AND T2.RTE_ID = T3.EXECUTION_AUDIT_ID
        --AND T3.EXECUTION_NAME like '%SOC_SAVEDESK_FCT_UPD'
    --order by T1.CREATION_DATE desc
)                                                                                          
select /*+ dynamic_sampling (4) gather_plan_statistics */  
   CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
   execution_audit_id,
   SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1) execution_name_short,
   DECODE (task_type,
           'PLSQL', 'Mapping',
           'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)    TYPE,
   q1.created_on,
   q1.updated_on, execution_audit_status,  return_result,
   ROUND ( (q1.updated_on - q1.created_on) * 24 * 60, 1) duration_mins,
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (q1.updated_on - q1.created_on) * 24 * 60, 1) ASC) OVER (partition by SYS_CONNECT_BY_PATH (
                                                                                    SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),'/')/*execution_name*/) duration_mins_p80,
   q3.MINS_MAP_ONLY_ALL_TARGETS,
   ROUND ( (q1.updated_on - q1.created_on) * 24 * 60 /*duration_mins*/ - q3.MINS_MAP_ONLY_ALL_TARGETS, 1)   MINS_IN_PRE_AND_POST_MAP,   
   q3.MINS_MAP_ONLY_SINGLE_TARGET,
   q3.RTD_TARGET,
   q3.RTD_NAME,        
   ROUND((q3.mapping_created_on_min - q1.created_on)* 24 * 60, 1) MINS_IN_PREMAPPING,
   ROUND((q1.updated_on - q3.mapping_updated_on_max)* 24 * 60, 1) MINS_IN_POSTMAPPING,                                                                                     
   ROUND((
            (   select c.updated_on 
                from owbsys.all_rt_audit_executions c 
                where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                      AND  c.execution_name like trim('%'||nvl('&&mstone_node_name','xxxxxx'))
            ) 
        - q1.created_on) * 24 , 1) hr_unt_end_of_mstone_incl_node,
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((
            (   select c.updated_on 
                from owbsys.all_rt_audit_executions c 
                where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                      AND  c.execution_name like trim('%'||nvl('&mstone_node_name','xxxxxx'))
            ) 
        - q1.created_on) * 24 , 1)  ASC) OVER (partition by SYS_CONNECT_BY_PATH (
                                                                                    SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),'/') /*execution_name*/) hrs_unt_end_of_mstone_p80,        
   ROUND((CONNECT_BY_ROOT q1.updated_on - q1.created_on) * 24 , 1) hr_until_end_of_root_incl_node,   
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((CONNECT_BY_ROOT q1.updated_on - q1.created_on) * 24 , 1)  ASC) OVER (partition by SYS_CONNECT_BY_PATH (
                                                                                    SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),'/') /*execution_name*/) hrs_until_end_of_root_p80,      
   CONNECT_BY_ROOT q1.execution_name root_execution_name,
   --b.critical_ind,
   ROUND ( (CONNECT_BY_ROOT q1.updated_on - CONNECT_BY_ROOT q1.created_on) * 24, 1) root_duration_hrs,
   CONNECT_BY_ROOT q1.created_on root_created_on,
   CONNECT_BY_ROOT q1.updated_on root_updated_on,
   CONNECT_BY_ISLEAF "IsLeaf",
   LEVEL,
   SYS_CONNECT_BY_PATH (
      SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),      '/') "Path" ,
    q3.RTA_PRIMARY_SOURCE,
    --q3.RTD_TARGET,
    q3.RTD_SELECT, q3.RTD_INSERT, q3.RTD_UPDATE, q3.RTD_DELETE, q3.RTD_MERGE   
--   q2.* 
from q1 --, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
--    LEFT OUTER JOIN q2
--        ON (SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1) = q2.map_name 
--            AND q2.start_time between q1.created_on AND q1.updated_on)
        LEFT OUTER JOIN q3
            ON (q1.execution_audit_id = q3.RTE_ID)                    
WHERE 1=1 
   --AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
   AND q1.execution_name like trim('%&&node_name')
   --AND CONNECT_BY_ISLEAF = 1 
   --AND execution_audit_status = 'COMPLETE'
   --AND return_result = 'OK'  
   --------AND CONNECT_BY_ROOT execution_name = b.flow_name
START WITH PARENT_EXECUTION_AUDIT_ID IS NULL
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
ORDER BY  root_execution_name, root_execution_audit_id DESC, execution_audit_id desc --, duration_mins DESC;

/************
    Find flows dependencies for KPIDW_MAIN
************/

with core
as (
    /*********************
    --   NMR Flows Dependencies (the most delayed flow is ordered first)
    *********************/
      SELECT rank() over(partition by EXECUTION_DATE order by END_EXECUTION desc) r, t.*
        FROM (
                SELECT 'KPIDW_MAIN' FLOW,
                     'NMR_GLOBAL_RUN_DATE' RELATED_DATE,
                     TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                     TASK_NAME,
                     CREATION_DATE START_EXECUTION,
                     LAST_UPDATE_DATE END_EXECUTION
                FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
               WHERE     TASK_NAME LIKE '%KPIDW_MAIN'
                     AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')    
              UNION ALL
                SELECT 'DELTA_EXTR_FCTS_CCRM' FLOW,
                     'CCRM_LAST_RUN' RELATED_DATE,
                     TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                     TASK_NAME,
                     CREATION_DATE START_EXECUTION,
                     LAST_UPDATE_DATE END_EXECUTION
                FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
               WHERE     TASK_NAME LIKE '%ADDVAL_DW_CONTROL'
                     AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
              UNION ALL
              SELECT 'SOC_DW_MAIN' FLOW,
                     'SOC_END_FORNMR_DATE' RELATED_DATE,
                     TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                     TASK_NAME,
                     CREATION_DATE START_EXECUTION,
                     LAST_UPDATE_DATE END_EXECUTION
                FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
               WHERE     TASK_NAME LIKE '%SOC_FLOWPROGR_ENDSOC_ETL'
                     AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
              UNION ALL
              SELECT 'LEVEL0_DAILY' FLOW,
                     'OTE_DW_LAST_RUN' RELATED_DATE,
                     TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                     TASK_NAME,
                     CREATION_DATE START_EXECUTION,
                     LAST_UPDATE_DATE END_EXECUTION
                FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
               WHERE     TASK_NAME LIKE '%DW_UPDATE_LAST_RUN_DATE'
                     AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
              UNION ALL
              SELECT 'PER_MAIN' FLOW,
                     'PERIF_NMR_END' RELATED_DATE,
                     TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                     TASK_NAME,
                     CREATION_DATE START_EXECUTION,
                     LAST_UPDATE_DATE END_EXECUTION
                FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
               WHERE     TASK_NAME LIKE '%FLOW_PROGRESS_NMREND_LOAD_STG'
                     AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
        ) t
        where flow = nvl('&&flow_name',flow)
    ORDER BY EXECUTION_DATE DESC, END_EXECUTION DESC
)
select R finish_order, flow, count(*) cnt
from core
group by R, flow
order by R, cnt desc;

/************
    Find flows dependencies for CTO_MAIN
************/

with core
as (
/*********************
--  CTO Flows Dependencies (the most delayed flow is ordered first)
*********************/
  SELECT rank() over(partition by EXECUTION_DATE order by END_EXECUTION desc) r, t.*
    FROM (
            SELECT 'CTO_MAIN' FLOW,
                 'CTO_LAST_RUN' RELATED_DATE,
                 TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                 TASK_NAME,
                 CREATION_DATE START_EXECUTION,
                 LAST_UPDATE_DATE END_EXECUTION
            FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
           WHERE     TASK_NAME LIKE '%CTO_MAIN'
                 AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')    
           UNION ALL
            SELECT 'CMP_RUN_DAILY:SBL_RUN' FLOW,
                 'FAULT_LAST_RUN' RELATED_DATE,
                 TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                 TASK_NAME,
                 CREATION_DATE START_EXECUTION,
                 LAST_UPDATE_DATE END_EXECUTION
            FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
           WHERE     TASK_NAME LIKE '%FAULTS_UPDATE_CTRL_AND_EMAIL'
                 AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
          UNION ALL
          SELECT 'CMP_RUN_DAILY:GNS_RUN' FLOW,
                 'GENESYS_LAST_RUN' RELATED_DATE,
                 TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                 TASK_NAME,
                 CREATION_DATE START_EXECUTION,
                 LAST_UPDATE_DATE END_EXECUTION
            FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
           WHERE     TASK_NAME LIKE 'GNS_RUN%MARK_FLOW_END'
                 AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
          UNION ALL
          SELECT 'WFM_RUN' FLOW,
                 'WFM_LAST_RUN' RELATED_DATE,
                 TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                 TASK_NAME,
                 CREATION_DATE START_EXECUTION,
                 LAST_UPDATE_DATE END_EXECUTION
            FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
           WHERE     TASK_NAME LIKE 'WFM_FL%MARK_FLOW_END'
                 AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
          UNION ALL
          SELECT 'SOC_DW_MAIN' FLOW,
                 'SOC_END_FORNMR_DATE' RELATED_DATE,
                 TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                 TASK_NAME,
                 CREATION_DATE START_EXECUTION,
                 LAST_UPDATE_DATE END_EXECUTION
            FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
           WHERE     TASK_NAME LIKE '%SOC_FLOWPROGR_ENDSOC_ETL'
                 AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
          UNION ALL
          SELECT 'LEVEL0_DAILY' FLOW,
                 'OTE_DW_LAST_RUN' RELATED_DATE,
                 TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                 TASK_NAME,
                 CREATION_DATE START_EXECUTION,
                 LAST_UPDATE_DATE END_EXECUTION
            FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
           WHERE     TASK_NAME LIKE '%DW_UPDATE_LAST_RUN_DATE'
                 AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
          UNION ALL
          SELECT 'PER_MAIN' FLOW,
                 'PERIF_PRESENT_AREA_END' RELATED_DATE,
                 TRUNC (LAST_UPDATE_DATE) EXECUTION_DATE,
                 TASK_NAME,
                 CREATION_DATE START_EXECUTION,
                 LAST_UPDATE_DATE END_EXECUTION
            FROM OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS
           WHERE     TASK_NAME LIKE '%FLOW_PRESENT_PROGRESS_LOAD_STG'
                 AND CREATION_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1  -- >= TO_DATE ('&&PARAM', 'DD/MM/RRRR')
        ) t
        where flow = nvl('&&flow_name',flow)
ORDER BY EXECUTION_DATE DESC, END_EXECUTION DESC
)
select R finish_order, flow, count(*) cnt
from core
group by R, flow
order by R, cnt desc;


/****************
    Query to find the execution of all major flows in the history of interest
    
  Parameters:
  flow_name    (optional) The Main flow name e.g. SOC_DW_MAIN
  critical_ind  (optional)  0/1 if the main flow is critical
  days_back     (optional)  Num of days back from sysdate of required history
  monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
  Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays    
    
*****************/

/**** NOTE ****
    when redefining the query remember to redefine the following view (owb_pa.sql script depends on this view)
    15 days and critical_ind = 1
*/
--CREATE OR REPLACE FORCE VIEW MONITOR_DW.V_DWP_FLOWS_EXEC_TIMES
--(
--   EXECUTION_NAME,
--   MONTHLY_RUN_IND,
--   MONDAY_RUN_IND,
--   CREATED_ON,
--   UPDATED_ON,
--   EXECUTION_AUDIT_STATUS,
--   RETURN_RESULT,
--   DUR_HRS_CLEAN,
--   P80_DUR_HRS_CLEAN,
--   DURATION_HRS_TOTAL,
--   DUR_HRS_WAITING,
--   DUR_HRS_ERROR,
--   PREV_7DAYS_HRS_CLEAN,
--   HRS_BENEFIT,
--   P80_DURATION_HRS_TOTAL,
--   P80_DUR_HRS_WAITING,
--   P80_DUR_HRS_ERROR,
--   TOTAL_ERROR_MINS,
--   TOT_MINS_FOR_RESOLVING_ERROR,
--   TOT_MINS_RUNNING_ON_ERROR,
--   CRITICAL_IND,
--   EXECUTION_AUDIT_ID,
--   TOP_LEVEL_EXECUTION_AUDIT_ID
--)
--AS
  select  execution_name,
          monthly_run_ind,
          monday_run_ind,
          created_on,
          updated_on,execution_audit_status, return_result,
          dur_hrs_clean,
          PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY dur_hrs_clean ASC) OVER (partition by execution_name) p80_dur_hrs_clean,                  
          duration_hrs_total,
          dur_hrs_waiting,
          dur_hrs_error,
          lag(dur_hrs_clean, 7) over (partition by execution_name order by created_on) prev_7days_hrs_clean,
          (lag(dur_hrs_clean, 7) over (partition by execution_name order by created_on) - dur_hrs_clean) hrs_benefit,         
          p80_duration_hrs_total,
          PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY dur_hrs_waiting ASC) OVER (partition by execution_name) p80_dur_hrs_waiting,
          PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY dur_hrs_error ASC) OVER (partition by execution_name) p80_dur_hrs_error,        
          TOTAL_ERROR_MINS,
          tot_mins_for_resolving_error,
          tot_mins_running_on_error,
          critical_ind,
          execution_audit_id,
          top_level_execution_audit_id
  from (        
      select  t1.execution_name,
              case when to_char(created_on, 'DD') = '01' then 1 else 0 end monthly_run_ind,
              case when trim(to_char(created_on, 'DAY')) = 'MONDAY' then 1 else 0 end monday_run_ind,        
              t1.created_on,
              t1.updated_on, execution_audit_status, return_result,
              t1.duration_hrs_total,
              (t1.duration_hrs_total - t1.duration_real_hrs) dur_hrs_waiting, 
              nvl(round((t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1),0) dur_hrs_error,
              case when -- xronos me error vgainei arnhtiko
                t1.duration_hrs_total - (t1.duration_hrs_total - nvl(t1.duration_real_hrs, t1.duration_hrs_total)) - (nvl(round((t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1),0)) < 0
                THEN    -- xronos xwris error
                        t1.duration_hrs_total - (t1.duration_hrs_total - nvl(t1.duration_real_hrs, t1.duration_hrs_total))
                ELSE -- xronos me error
                t1.duration_hrs_total - (t1.duration_hrs_total - nvl(t1.duration_real_hrs, t1.duration_hrs_total)) - (nvl(round((t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1),0))                                          
               END  dur_hrs_clean,        
              t1.duration_hrs_total_p80 p80_duration_hrs_total,
            --  t1.duration_real_hrs,
            --  t1.duration_real_hrs_p80,
            --  NVL(ROUND(t1.duration_real_hrs - (t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1), t1.duration_real_hrs) "dur_real_hrs_WITHOUT_errors",
            --  PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY NVL(ROUND(t1.duration_real_hrs - (t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1), t1.duration_real_hrs) ASC) OVER (partition by execution_name) "dur_real_hrs_WITHOUT_err_p80",
              (t2.tot_mins_for_resolving + t2.tot_mins_running_on_error) TOTAL_ERROR_MINS,        
              t2.tot_mins_for_resolving tot_mins_for_resolving_error, 
              t2.tot_mins_running_on_error,
              t1.critical_ind,
              t1.execution_audit_id,
              t1.top_level_execution_audit_id
      from (
          select execution_audit_id, top_level_execution_audit_id,
            created_on,
             updated_on, execution_audit_status, return_result,
             ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
             PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - created_on) * 24 , 1) ASC) OVER (partition by execution_name) duration_hrs_total_p80,       
             case when execution_name = 'KPIDW_MAIN'
                      THEN  round( ( updated_on - created_on - (  select updated_on - created_on
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
                      THEN  round( ( updated_on - created_on
                                                        - (  select updated_on - created_on
                                                      from owbsys.all_rt_audit_executions tt 
                                                      where
                                                          tt.execution_name like 'CTO_MAIN:CHECK_FLOWSEND_FORCTO_PROC'
                                                          AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                          AND execution_audit_status = 'COMPLETE'
                                                          AND return_result = 'OK'
                                                          )                        
                                                - ( -- trexoyn parallhla ta 3 subflows me ksexoristes anamones - vres to pio argo
                                                    select GREATEST(                                                                                                                 
                                                                 (   select updated_on - created_on d
                                                                      from owbsys.all_rt_audit_executions tt 
                                                                      where
                                                                          tt.execution_name like 'CTO_MAIN:CHECK_FLOW_FORCTOWFM_PROC'
                                                                          AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                          AND execution_audit_status = 'COMPLETE'
                                                                          AND return_result = 'OK'
                                                                  ),
                                                                 (   select updated_on - created_on
                                                                      from owbsys.all_rt_audit_executions tt 
                                                                      where
                                                                          tt.execution_name like 'CTO_MAIN:CHECK_FLOW_FORCTOORDER_PROC'
                                                                          AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                          AND execution_audit_status = 'COMPLETE'
                                                                          AND return_result = 'OK'
                                                                  ),
                                                                 (   select updated_on - created_on
                                                                      from owbsys.all_rt_audit_executions tt 
                                                                      where
                                                                          tt.execution_name like 'CTO_MAIN:CHECK_FLOW_FORCTOFAULT_PROC'
                                                                          AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                          AND execution_audit_status = 'COMPLETE'
                                                                          AND return_result = 'OK'
                                                                  )                                                         
                                                         )  from dual  
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
                                        - ( -- this returns no rows for non-monthly flows. Use sum to fix it
                                                select nvl(sum(updated_on - created_on),0)
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'PER_LOADKPIS_MON_SNP:CHECK_DWH_ENDKPIDWGLOBAL_PROC'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                      
                                        )
                                        - (select updated_on - created_on 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like 'PER_LOADPRESENT_OPERREP:PRESENT_CHECK_ORDPEND_END_PROC'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                  
                                        )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                                    )*24  -- hours
                            ,1)
                  when execution_name = 'CMP_RUN_DAILY' 
                      THEN  round( ( updated_on - created_on
                                        - (select nvl(sum(updated_on - created_on),0) 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like 'CMP_RUN_DAILY:PRERUNCHECK'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                  
                                            )
                                        - (select nvl(max(updated_on - created_on),0) 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name in ('CMP_RUN_DAILY:PRERUNCHECK_SBL', 'CMP_RUN_DAILY:PRERUNCHECK_GNS')
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                  
                                            )
                                        - (  select nvl(max(updated_on - created_on),0)   
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'FAULT_FCTS_TRG:CHECK_FLOW_WFM_FINNISH'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                                                 
                                       )
                                    )*24  -- hours
                            ,1)
                  when execution_name = 'CMP_RUN_DAILY:SBL_RUN' 
                      THEN  round( ( updated_on - created_on
                                        - (select nvl(sum(updated_on - created_on),0) 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like '%:CHECK_FLOW_WFM_FINNISH'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                  
                                            )
                                        - (  select nvl(max(updated_on - created_on),0)   
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like '%:CHECK_NP_VIEW_LOADED'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                                                 
                                       )
                                        - (  select nvl(max(updated_on - created_on),0)   
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like '%:PRESTAGE_CHECK_UAE_PROC'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                                                 
                                       )
                                        - (  select nvl(max(updated_on - created_on),0)   
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like '%:CHECK_LVL0_DIM_END'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                                                 
                                       )
                                        - (  select nvl(max(updated_on - created_on),0)   
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like '%:CHECK_CRM_LVL0_SHADOW_END'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                                                 
                                       )                                                                                                                                                                                                                                                                                 
                                    )*24  -- hours
                            ,1)
                  when execution_name = 'CMP_RUN_DAILY:GNS_RUN' 
                      THEN  round( ( updated_on - created_on
                                        - (  select nvl(max(updated_on - created_on),0)   
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like '%:CHECK_GNS_LVL0_SHADOW_END'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
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
                  when execution_name = 'PENDORD_RUN' 
                              THEN  round( ( updated_on - created_on
                                                - (select sum(updated_on - created_on) 
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'PENDORD_RUN:PRERUNCHECK'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                  
                                                    )
                                                - (select sum(updated_on - created_on) 
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'PENDORD_RUN:BUS_PRERUN_CHECK'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                  
                                                    )                                                                                                                              
                                            )*24  -- hours
                                    ,1) 
                  when execution_name = 'PATCH_UNBOUNDED_OL'
                      THEN  round( ( updated_on - (  select updated_on 
                                              from owbsys.all_rt_audit_executions tt 
                                              where
                                                  tt.execution_name like 'PATCH_UNBOUNDED_OL:INITIALIZE_PROC'
                                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                  AND execution_audit_status = 'COMPLETE'
                                                  AND return_result = 'OK'
                                                  ) 
                                    )*24  -- hours
                            ,1)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                  ELSE null
              END duration_real_hrs, -- afairoume tous xronous anamonhs  
             PERCENTILE_CONT(0.8) WITHIN GROUP (
                  ORDER BY 
                      case when execution_name = 'KPIDW_MAIN'
                                      THEN  round( ( updated_on - created_on 
                                                            - (  select updated_on - created_on
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
                                      THEN  round( ( updated_on - created_on 
                                                                - (  select updated_on - created_on
                                                              from owbsys.all_rt_audit_executions tt 
                                                              where
                                                                  tt.execution_name like 'CTO_MAIN:CHECK_FLOWSEND_FORCTO_PROC'
                                                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                  AND execution_audit_status = 'COMPLETE'
                                                                  AND return_result = 'OK'
                                                                  )                        
                                                        - ( -- trexoyn parallhla ta 3 subflows me ksexoristes anamones - vres to pio argo
                                                            select GREATEST(                                                                                                                 
                                                                         (   select updated_on - created_on d
                                                                              from owbsys.all_rt_audit_executions tt 
                                                                              where
                                                                                  tt.execution_name like 'CTO_MAIN:CHECK_FLOW_FORCTOWFM_PROC'
                                                                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                                  AND execution_audit_status = 'COMPLETE'
                                                                                  AND return_result = 'OK'
                                                                          ),
                                                                         (   select updated_on - created_on
                                                                              from owbsys.all_rt_audit_executions tt 
                                                                              where
                                                                                  tt.execution_name like 'CTO_MAIN:CHECK_FLOW_FORCTOORDER_PROC'
                                                                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                                  AND execution_audit_status = 'COMPLETE'
                                                                                  AND return_result = 'OK'
                                                                          ),
                                                                         (   select updated_on - created_on
                                                                              from owbsys.all_rt_audit_executions tt 
                                                                              where
                                                                                  tt.execution_name like 'CTO_MAIN:CHECK_FLOW_FORCTOFAULT_PROC'
                                                                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                                  AND execution_audit_status = 'COMPLETE'
                                                                                  AND return_result = 'OK'
                                                                          )                                                         
                                                                 )  from dual  
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
                                                        - ( -- this returns no rows for non-monthly flows. Use sum to fix it
                                                            select nvl(sum(updated_on - created_on),0)
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'PER_LOADKPIS_MON_SNP:CHECK_DWH_ENDKPIDWGLOBAL_PROC'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                  
                                                             )    
                                                        - (select updated_on - created_on 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'PER_LOADPRESENT_OPERREP:PRESENT_CHECK_ORDPEND_END_PROC'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                  
                                                        )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                                                    )*24  -- hours
                                            ,1)
                                  when execution_name = 'CMP_RUN_DAILY' 
                                      THEN  round( ( updated_on - created_on
                                                        - (select nvl(sum(updated_on - created_on),0) 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'CMP_RUN_DAILY:PRERUNCHECK'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                  
                                                            )
                                                        - (select nvl(max(updated_on - created_on),0) 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name in ('CMP_RUN_DAILY:PRERUNCHECK_SBL', 'CMP_RUN_DAILY:PRERUNCHECK_GNS')
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                  
                                                            )
                                                        - (  select nvl(max(updated_on - created_on),0)   
                                                                from owbsys.all_rt_audit_executions tt 
                                                                where
                                                                    tt.execution_name like 'FAULT_FCTS_TRG:CHECK_FLOW_WFM_FINNISH'
                                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                    AND execution_audit_status = 'COMPLETE'
                                                                    AND return_result = 'OK'                                                                                                                 
                                                       )
                                                    )*24  -- hours
                                            ,1)
                                  when execution_name = 'CMP_RUN_DAILY:SBL_RUN' 
                                      THEN  round( ( updated_on - created_on
                                                        - (select nvl(sum(updated_on - created_on),0) 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like '%:CHECK_FLOW_WFM_FINNISH'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID 
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                  
                                                            )
                                                        - (  select nvl(max(updated_on - created_on),0)   
                                                                from owbsys.all_rt_audit_executions tt 
                                                                where
                                                                    tt.execution_name like '%:CHECK_NP_VIEW_LOADED'
                                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                                    AND execution_audit_status = 'COMPLETE'
                                                                    AND return_result = 'OK'                                                                                                                 
                                                       )
                                                        - (  select nvl(max(updated_on - created_on),0)   
                                                                from owbsys.all_rt_audit_executions tt 
                                                                where
                                                                    tt.execution_name like '%:PRESTAGE_CHECK_UAE_PROC'
                                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                                    AND execution_audit_status = 'COMPLETE'
                                                                    AND return_result = 'OK'                                                                                                                 
                                                       )
                                                        - (  select nvl(max(updated_on - created_on),0)   
                                                                from owbsys.all_rt_audit_executions tt 
                                                                where
                                                                    tt.execution_name like '%:CHECK_LVL0_DIM_END'
                                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                                    AND execution_audit_status = 'COMPLETE'
                                                                    AND return_result = 'OK'                                                                                                                 
                                                       )
                                                        - (  select nvl(max(updated_on - created_on),0)   
                                                                from owbsys.all_rt_audit_executions tt 
                                                                where
                                                                    tt.execution_name like '%:CHECK_CRM_LVL0_SHADOW_END'
                                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
                                                                    AND execution_audit_status = 'COMPLETE'
                                                                    AND return_result = 'OK'                                                                                                                 
                                                       )                                                                                                                                                                                                                                                                                                                                                                                                                                                             
                                                    )*24  -- hours
                                            ,1)  
                                  when execution_name = 'CMP_RUN_DAILY:GNS_RUN' 
                                      THEN  round( ( updated_on - created_on
                                                            - (  select nvl(max(updated_on - created_on),0)   
                                                            from owbsys.all_rt_audit_executions tt 
                                                            where
                                                                tt.execution_name like '%:CHECK_GNS_LVL0_SHADOW_END'
                                                                AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.TOP_LEVEL_EXECUTION_AUDIT_ID
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
                                  when execution_name = 'PATCH_UNBOUNDED_OL'
                                      THEN  round( ( updated_on - (  select updated_on 
                                                              from owbsys.all_rt_audit_executions tt 
                                                              where
                                                                  tt.execution_name like 'PATCH_UNBOUNDED_OL:INITIALIZE_PROC'
                                                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                  AND execution_audit_status = 'COMPLETE'
                                                                  AND return_result = 'OK'
                                                                  ) 
                                                    )*24  -- hours
                                            ,1)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                  ELSE null
                              END                         
                  ASC) 
                  OVER (partition by execution_name) duration_real_hrs_p80,                               
             execution_name,
             b.critical_ind
          from owbsys.all_rt_audit_executions a, monitor_dw.dwp_etl_flows b
          where
              a.execution_name = flow_name
              AND flow_name = nvl('&&flow_name',flow_name)
              AND critical_ind = nvl('&&critical_ind',critical_ind)
              and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
              --and created_on > sysdate - &&days_back
              AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
              AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
             -- AND execution_audit_status = 'COMPLETE'
             -- AND return_result = 'OK'
             --order by  execution_name, CREATED_ON desc 
          UNION
          -- the time needed for'SOC_FCTS_TRG:SOC_FLOWPROGR_ENDSOC_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης SOC για NMR
          select execution_audit_id, top_level_execution_audit_id, root_created_on created_on,
             updated_on updated_on,
             execution_audit_status, return_result,
             ROUND ( (updated_on - root_created_on) * 24 , 1) duration_hrs_total,
             PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_hrs_total_p80,       
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
             PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY                                   
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
                                                ASC) OVER () duration_real_hrs_p80,
             'SOC_for_NMR' execution_name,
             1    CRITICAL_IND
          from (
              with q1 as ( 
                  -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
                  select /*+ materialize dynamic_sampling (4) */  execution_audit_id, top_level_execution_audit_id, parent_execution_audit_id,
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
                                                          and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                                                          --and created_on > sysdate - &&days_back
                                                          --AND execution_audit_status = 'COMPLETE'
                                                          --AND return_result = 'OK'                                                        
                                                      )
                  -- restricitons for all the nodes (not just the root)    
                  --AND CREATED_ON > SYSDATE - (&days_back)
                  and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                  AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
                  AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
                 -- AND a.execution_audit_status = 'COMPLETE'
                  --AND a.return_result = 'OK'
              )                                                                                           
              select /*+ dynamic_sampling (4) */  
                 CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
                  execution_audit_id, top_level_execution_audit_id,
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
                 'SOC_for_NMR' = nvl('&&flow_name','SOC_for_NMR')
                 --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
                 AND execution_name like '%SOC_FLOWPROGR_ENDSOC_ETL' 
                 AND CONNECT_BY_ISLEAF = 1 
                 AND execution_audit_status = 'COMPLETE'
                 AND return_result = 'OK'  
                 --------AND CONNECT_BY_ROOT execution_name = b.flow_name
              START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
              CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
               --ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
          ) t
          --order by updated_on desc 
          UNION
          -- the time needed for dims to be loaded into shadow
          select execution_audit_id, top_level_execution_audit_id, root_created_on created_on,
                 updated_on updated_on, execution_audit_status, return_result,
                 ROUND ( (updated_on - root_created_on) * 24 , 1) duration_hrs_total,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_hrs_total_p80,           
                 ROUND ( (updated_on - root_created_on) * 24 , 1) duration_real_hrs,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_real_hrs_p80,
                 'LOAD_DIMS_SHADOW' execution_name,
                 1 critical_ind
          from (
          with q1 as ( 
              -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
              select /*+ materialize dynamic_sampling (4) */  execution_audit_id, top_level_execution_audit_id, parent_execution_audit_id,
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
                                                      and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                                                      --and created_on > sysdate - &&days_back
                                                     -- AND execution_audit_status = 'COMPLETE'
                                                     -- AND return_result = 'OK'                                                        
                                                  )
              -- restricitons for all the nodes (not just the root)    
              --AND CREATED_ON > SYSDATE - (&&days_back)
              and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
              AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
              AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end       
            -- AND a.execution_audit_status = 'COMPLETE'
            -- AND a.return_result = 'OK'
          )                                                                                           
          select /*+ dynamic_sampling (4) */  
             CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
              execution_audit_id, top_level_execution_audit_id,
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
             'LOAD_DIMS_SHADOW' = nvl('&&flow_name', 'LOAD_DIMS_SHADOW')
             --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
             AND execution_name like '%PERIF_FLOWPROGR_ENDSHADOW_ETL' 
             AND CONNECT_BY_ISLEAF = 1 
             AND execution_audit_status = 'COMPLETE'
             AND return_result = 'OK'  
             --------AND CONNECT_BY_ROOT execution_name = b.flow_name
          START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
          CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
           --ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
          ) t
          --order by end_date desc                     
      ) t1,
      (      
      --**** Calculate the time spent on error for a major Flow
      select  nvl(sum(mins_for_resolving),0) tot_mins_for_resolving,
              nvl(sum(mins_running_on_error),0) tot_mins_running_on_error, 
              root_execution_name, root_created_on, root_updated_on, root_execution_audit_id         
      from
      (   -- detailed query showing the individual mappings on error
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
                                                      --and created_on > sysdate - &&days_back
                                                      and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                                                      --AND execution_audit_status = 'COMPLETE'
                                                      --AND return_result = 'OK'                                                        
                                                  )
              -- restricitons for all the nodes (not just the root)    
              --AND CREATED_ON > SYSDATE - (&days_back)
              and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1    
               AND to_char(created_on, 'DD') = case when nvl('&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
               AND trim(to_char(created_on, 'DAY')) = case when nvl('&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
             --AND a.execution_audit_status = 'COMPLETE'
             --AND a.return_result = 'OK'
          )
          select  case when tt.result = 'OK' THEN ROUND((TT.CREATED_ON - tt.PREV_UPDATED_ON) * 24 * 60, 1) else null end mins_for_resolving, --ROUND((TT.CREATED_ON - tt.PREV_UPDATED_ON) * 24 * 60, 1) mins_for_resolving,
                  case when tt.result in ('FAILURE', 'OK_WITH_WARNINGS') THEN  ROUND((TT.UPDATED_ON - tt.CREATED_ON) * 24 * 60, 1) else null end mins_running_on_error,
                  tt.*
          from (                                                                                        
              select /*+ dynamic_sampling (4) */  
                 -- label rows where result = 'failure' or the 1st OK result after a failure
                 case when (q1.return_result in ('OK') AND lag(return_result,1) over (partition by CONNECT_BY_ROOT execution_audit_id, execution_name order by q1.updated_on ) in ('FAILURE', 'OK_WITH_WARNINGS')) THEN 1
                      when  q1.return_result in ('FAILURE', 'OK_WITH_WARNINGS') THEN 1
                      ELSE 0
                 end ind,           
                 CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
                 execution_audit_id,
                 CONNECT_BY_ROOT execution_name root_execution_name,   
                 execution_name,
                 SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
                 DECODE (task_type,
                         'PLSQL', 'Mapping',
                         'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)    TYPE,
                 b.CREATION_DATE error_creation_date,
                  q1.RETURN_RESULT RESULT,
                  b.SEVERITY,
                  c.PLAIN_TEXT,        
                 created_on,
                 updated_on,
                 lag(updated_on,1) over (partition by CONNECT_BY_ROOT execution_audit_id, execution_name order by q1.updated_on) prev_updated_on, 
                 execution_audit_status,  return_result,
                 ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - created_on) * 24 * 60, 1) ASC) OVER (partition by execution_name) duration_mins_p80,   
                 ROUND((
                          (   select c.updated_on 
                              from owbsys.all_rt_audit_executions c 
                              where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                                    AND  c.execution_name like trim('%'||nvl('&&mstone_node_name','xxxxxx'))
                          ) 
                      - created_on) * 24 , 1) hr_unt_end_of_mstone_incl_node,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((
                          (   select c.updated_on 
                              from owbsys.all_rt_audit_executions c 
                              where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                                    AND  c.execution_name like trim('%'||nvl('&mstone_node_name','xxxxxx'))
                          ) 
                      - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_unt_end_of_mstone_p80,        
                 ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1) hr_until_end_of_root_incl_node,   
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_until_end_of_root_p80,      
                 --b.critical_ind,
                 ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
                 CONNECT_BY_ROOT created_on root_created_on,
                 CONNECT_BY_ROOT updated_on root_updated_on,
                 CONNECT_BY_ISLEAF "IsLeaf",
                 LEVEL,
                 SYS_CONNECT_BY_PATH (
                    SUBSTR (execution_name, INSTR (execution_name, ':') + 1),      '/') path  
              from q1,owbsys.WB_RT_AUDIT_MESSAGES b, owbsys.WB_RT_AUDIT_MESSAGE_LINES c
              WHERE
                 q1.EXECUTION_AUDIT_ID= b.AUDIT_EXECUTION_ID(+)  
                 AND  b.AUDIT_MESSAGE_ID = c.AUDIT_MESSAGE_ID(+)
                 AND q1.return_result in ('FAILURE', 'OK','OK_WITH_WARNINGS') 
                 AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
                 --execution_name like trim('%&node_name')
                 AND CONNECT_BY_ISLEAF = 1 
                 --AND execution_audit_status = 'COMPLETE'
                 --AND return_result = 'OK'  
                 --------AND CONNECT_BY_ROOT execution_name = b.flow_name
              START WITH PARENT_EXECUTION_AUDIT_ID IS NULL
              CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
          ) tt
          where
              tt.ind = 1
          --ORDER BY root_execution_name, root_execution_audit_id DESC, path, error_creation_date desc--, duration_mins DESC;
      ) ttt
      where
          mins_for_resolving > 0 or mins_running_on_error > 0
      group by root_execution_name, root_created_on, root_updated_on, root_execution_audit_id
      --order by  root_execution_name, root_created_on desc    
      ) t2
      where
          t1.execution_audit_id = t2.root_execution_audit_id(+)     
    --  order by  execution_name, CREATED_ON desc
  ) final
  order by  execution_name, CREATED_ON desc;
  
/***********************************************************************
    Query to find (per day) the order by which the flows finish (first the most delayed)
************************************************************************/  

--select pattern, count(distinct pattern)
--from (
--    -- detail

    select  rank() over(partition by trunc(created_on) order by updated_on desc) r, 
--            listagg(execution_name, '/') within group (order by updated_on desc) over(partition by trunc(created_on)) pattern,
            flows.*
    from (
        MONITOR_DW.V_DWP_FLOWS_EXEC_TIMES
    ) flows
    order by trunc(created_on) desc, r;
     
--) detail
--group by pattern
--order by count(distinct pattern) desc;


/*********************************************************************
 Query to find the executions per day of the whole tree

Notes:
    %CHECK% nodes are excluded

    If you specify a node_name then the tree will start with root this node_name
    
    If you change the ORDER SIBLINGS BY to CREATED_ON ASC then  you can sort sibling nodes by creation time
**********************************************************************/
--select *
--from (     
       
    with q1 as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  
            execution_audit_id, 
            parent_execution_audit_id,
            TOP_LEVEL_EXECUTION_AUDIT_ID,
            execution_name,
            execution_audit_status,                    
            return_result,
            task_type,
            created_on,
            updated_on,
            row_number() over(partition by execution_name order by created_on) r,
            PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,
            round(avg((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) avg_duration_mins,
            round(stddev((updated_on - created_on) * 24 * 60) OVER (partition by execution_name) ,1) stddev_duration_mins,
            round(min((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) min_duration_mins,
            round(max((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) max_duration_mins,
            count(distinct TOP_LEVEL_EXECUTION_AUDIT_ID||execution_name) OVER (partition by execution_name) numof_executions        
        from  owbsys.all_rt_audit_executions a
        where
            TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flow of interest and criticality of interest(get the last run (sysdate - 1))
                                             select execution_audit_id
                                             from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                             where 
                                                execution_name = flow_name
                                                and PARENT_EXECUTION_AUDIT_ID IS NULL
                                                AND flow_name = nvl('&&flow_name',flow_name)
                                                AND critical_ind = nvl('&&critical_ind',critical_ind)
                                                -- restrictions for the main flow
                                                AND CREATED_ON > SYSDATE - (&&days_back)    
                                                AND to_char(created_on, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
                                                AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
                                               -- AND execution_audit_status = 'COMPLETE'
                                              --  AND return_result = 'OK'                                                        
                                            )
            -- restricitons for all the nodes (not just the root) 
            AND CREATED_ON > SYSDATE - (&&days_back)    
            AND to_char(created_on, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
            AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
           -- AND   execution_audit_status = 'COMPLETE'
           -- AND return_result = 'OK'                     
    )                                                                                           
    select /*+ dynamic_sampling (4) */  
        CONNECT_BY_ROOT execution_name root_execution_name,
        r,
        created_on,
        updated_on,
        execution_audit_status,
        return_result,        
        level,
        lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(execution_name, '_\d\d$', ''),'\w+:' , '' ) execution_name_short,    
        DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)
        TYPE,
        lpad(' ', 2*(level - 1),'   ')||ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
        lpad(' ', 2*(level - 1),'   ')||p80_duration_mins p80_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||avg_duration_mins avg_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||stddev_duration_mins stddev_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||min_duration_mins min_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||max_duration_mins max_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||numof_executions numof_executions,
        --PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,        
        LEVEL owb_level,    
        CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,        
        execution_audit_id,
        execution_name,
       ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) mins_until_end_of_root,
       --b.critical_ind,
       ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
       CONNECT_BY_ROOT created_on root_created_on,
       CONNECT_BY_ROOT updated_on root_updated_on,
       CONNECT_BY_ISLEAF "IsLeaf",
       SYS_CONNECT_BY_PATH (
          SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
          '/')
          path  
    from q1 a --, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
    WHERE  1=1
       --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
       --AND CONNECT_BY_ISLEAF = 1   
       --AND CONNECT_BY_ROOT execution_name = b.flow_name
      --  SUBSTR (execution_name, INSTR (execution_name, ':') + 1) NOT LIKE '%CHECK%' -- exclude "check" nodes
       --AND execution_name like nvl(trim('%&&node_name'),execution_name)
    START WITH execution_name like '%'||nvl(trim('&node_name'), trim('&&flow_name')) --a.PARENT_EXECUTION_AUDIT_ID IS NULL                
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
    ORDER SIBLINGS BY a.TOP_LEVEL_EXECUTION_AUDIT_ID desc, (updated_on - created_on) DESC --a.p80_DURATION_MINS DESC  --a.created_on asc, a.p80_DURATION_MINS DESC 

--)
--where 1=1
  --  and type in ('Procedure', 'Mapping', 'ProcessFlow', 'Function', 'Shell')
     -- 15 minutes threshold
--    and duration_mins > 15;


/***************************************************************
Find mappings on error for a specific main flow (i.e., return result <> OK)
****************************************************************/

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
   CONNECT_BY_ROOT execution_name root_execution_name,   
   SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
   DECODE (task_type,
           'PLSQL', 'Mapping',
           'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)    TYPE,
   b.CREATION_DATE error_creation_date,
    q1.RETURN_RESULT,
    b.SEVERITY,
    c.PLAIN_TEXT,        
   created_on,
   updated_on, execution_audit_status,  return_result,
   ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - created_on) * 24 * 60, 1) ASC) OVER (partition by execution_name) duration_mins_p80,   
   ROUND((
            (   select c.updated_on 
                from owbsys.all_rt_audit_executions c 
                where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                      AND  c.execution_name like trim('%'||nvl('&&mstone_node_name','xxxxxx'))
            ) 
        - created_on) * 24 , 1) hr_unt_end_of_mstone_incl_node,
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((
            (   select c.updated_on 
                from owbsys.all_rt_audit_executions c 
                where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                      AND  c.execution_name like trim('%'||nvl('&mstone_node_name','xxxxxx'))
            ) 
        - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_unt_end_of_mstone_p80,        
   ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1) hr_until_end_of_root_incl_node,   
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_until_end_of_root_p80,      
   --b.critical_ind,
   ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
   CONNECT_BY_ROOT created_on root_created_on,
   CONNECT_BY_ROOT updated_on root_updated_on,
   CONNECT_BY_ISLEAF "IsLeaf",
   LEVEL,
   SYS_CONNECT_BY_PATH (
      SUBSTR (execution_name, INSTR (execution_name, ':') + 1),      '/') path  
from q1,owbsys.WB_RT_AUDIT_MESSAGES b, owbsys.WB_RT_AUDIT_MESSAGE_LINES c
WHERE 1=1
   AND q1.EXECUTION_AUDIT_ID= b.AUDIT_EXECUTION_ID(+)  
   AND  b.AUDIT_MESSAGE_ID = c.AUDIT_MESSAGE_ID(+) 
   AND nvl(q1.return_result,'lalala') <> 'OK' -- = 'FAILURE'  
   AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
   --execution_name like trim('%&node_name')
   AND CONNECT_BY_ISLEAF = 1 
   --AND execution_audit_status = 'COMPLETE'
   --AND return_result = 'OK'  
   --------AND CONNECT_BY_ROOT execution_name = b.flow_name
START WITH PARENT_EXECUTION_AUDIT_ID IS NULL
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
ORDER BY error_creation_date desc --root_execution_name, root_execution_audit_id DESC, error_creation_date desc--, duration_mins DESC;


--SELECT  execution_audit_id,
--           SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
--           DECODE (task_type,
--                   'PLSQL', 'Mapping',
--                   'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
--              TYPE,
--           b.CREATION_DATE error_creation_date,
--           execution_audit_status,
--           a.RETURN_RESULT,
--           b.SEVERITY,
--           c.PLAIN_TEXT,           
--           created_on,
--           updated_on,
--           ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
--            owbsys.wb_rt_constants.to_string (severity),
--              a.OBJECT_NAME,
--              --a.TASK_OBJECT_STORE_NAME,
--              a.TASK_NAME,
--           CONNECT_BY_ISLEAF "IsLeaf",
--           LEVEL,
--           SYS_CONNECT_BY_PATH (
--              SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
--              '/')
--              "Path",
--           CONNECT_BY_ROOT execution_name root_execution_name,           
--           CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
--           CONNECT_BY_ROOT created_on root_created_on,
--           CONNECT_BY_ROOT updated_on root_updated_on
--FROM owbsys.all_rt_audit_executions a,   owbsys.WB_RT_AUDIT_MESSAGES b,
--  owbsys.WB_RT_AUDIT_MESSAGE_LINES c
--WHERE   A.CREATED_ON > SYSDATE - (&days_back)
--    --AND a.execution_audit_status <> 'COMPLETE'
--    AND a.return_result <> 'OK'
--    AND CONNECT_BY_ISLEAF = 1
--    AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
--    AND a.EXECUTION_AUDIT_ID= b.AUDIT_EXECUTION_ID(+)  
--    AND  b.AUDIT_MESSAGE_ID = c.AUDIT_MESSAGE_ID(+) 
--START WITH  a.PARENT_EXECUTION_AUDIT_ID IS NULL
--            AND a.EXECUTION_NAME  = nvl(trim('&major_flow_name'), a.EXECUTION_NAME) 
--            --AND a.execution_audit_status <> 'COMPLETE'            --AND a.return_result = 'OK'
--CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
--order by b.CREATION_DATE desc



/***************************
 Calculate the time spent on error for a major Flow
****************************/

select  nvl(sum(mins_for_resolving),0) tot_mins_for_resolving,
        nvl(sum(mins_running_on_error),0) tot_mins_running_on_error, 
        root_execution_name, root_created_on, root_updated_on, root_execution_audit_id         
from
(   -- detailed query showing the individual mappings on error
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
    select  case when tt.result = 'OK' THEN ROUND((TT.CREATED_ON - tt.PREV_UPDATED_ON) * 24 * 60, 1) else null end mins_for_resolving,
            case when tt.result in ('FAILURE', 'OK_WITH_WARNINGS') THEN  ROUND((TT.UPDATED_ON - tt.CREATED_ON) * 24 * 60, 1) else null end mins_running_on_error,
            tt.*
    from (                                                                                        
        select /*+ dynamic_sampling (4) */  
           -- label rows where result = 'failure' or the 1st OK result after a failure
           case when (q1.return_result in ('OK') AND lag(return_result,1) over (partition by CONNECT_BY_ROOT execution_audit_id, execution_name order by q1.updated_on ) in ('FAILURE', 'OK_WITH_WARNINGS')) THEN 1
                when  q1.return_result in ('FAILURE', 'OK_WITH_WARNINGS') THEN 1
                ELSE 0
           end ind,           
           CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
           execution_audit_id,
           CONNECT_BY_ROOT execution_name root_execution_name,   
           execution_name,
           SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
           DECODE (task_type,
                   'PLSQL', 'Mapping',
                   'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)    TYPE,
           b.CREATION_DATE error_creation_date,
            q1.RETURN_RESULT RESULT,
            b.SEVERITY,
            c.PLAIN_TEXT,        
           created_on,
           updated_on,
           lag(updated_on,1) over (partition by CONNECT_BY_ROOT execution_audit_id, execution_name order by q1.updated_on) prev_updated_on, 
           execution_audit_status,  return_result,
           ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
           PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - created_on) * 24 * 60, 1) ASC) OVER (partition by execution_name) duration_mins_p80,   
           ROUND((
                    (   select c.updated_on 
                        from owbsys.all_rt_audit_executions c 
                        where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                              AND  c.execution_name like trim('%'||nvl('&&mstone_node_name','xxxxxx'))
                    ) 
                - created_on) * 24 , 1) hr_unt_end_of_mstone_incl_node,
           PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((
                    (   select c.updated_on 
                        from owbsys.all_rt_audit_executions c 
                        where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                              AND  c.execution_name like trim('%'||nvl('&mstone_node_name','xxxxxx'))
                    ) 
                - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_unt_end_of_mstone_p80,        
           ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1) hr_until_end_of_root_incl_node,   
           PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_until_end_of_root_p80,      
           --b.critical_ind,
           ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
           CONNECT_BY_ROOT created_on root_created_on,
           CONNECT_BY_ROOT updated_on root_updated_on,
           CONNECT_BY_ISLEAF "IsLeaf",
           LEVEL,
           SYS_CONNECT_BY_PATH (
              SUBSTR (execution_name, INSTR (execution_name, ':') + 1),      '/') path  
        from q1,owbsys.WB_RT_AUDIT_MESSAGES b, owbsys.WB_RT_AUDIT_MESSAGE_LINES c
        WHERE
           q1.EXECUTION_AUDIT_ID= b.AUDIT_EXECUTION_ID(+)  
           AND  b.AUDIT_MESSAGE_ID = c.AUDIT_MESSAGE_ID(+)
           AND q1.return_result in ('FAILURE', 'OK', 'OK_WITH_WARNINGS')
           --AND nvl(q1.return_result,'lalala') <> 'OK' 
           AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
           --execution_name like trim('%&node_name')
           AND CONNECT_BY_ISLEAF = 1 
           --AND execution_audit_status = 'COMPLETE'
           --AND return_result = 'OK'  
           --------AND CONNECT_BY_ROOT execution_name = b.flow_name
        START WITH PARENT_EXECUTION_AUDIT_ID IS NULL
        CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
    ) tt
    where
        tt.ind = 1
    ORDER BY root_execution_name, root_execution_audit_id DESC, path, error_creation_date desc--, duration_mins DESC;
) ttt
where
    mins_for_resolving > 0 or mins_running_on_error > 0
group by root_execution_name, root_created_on, root_updated_on, root_execution_audit_id
order by  root_execution_name, root_created_on desc

/*
    Find the procedure or package
*/
select *
from dba_procedures
where
 owner in ('ETL_DW', 'PERIF') 
 and (procedure_name in (upper('&&pname')) or object_name in (upper('&pname')))


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
