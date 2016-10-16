/*********************************************************************************************
    GOAL -->
    
    Καλησπέρα Νίκο,

Θα ήθελα σε παρακαλώ, αν μπορείς να φτιάξουμε ένα select, το οποίο θα μου επιστρέφει το πλήθος των errors στις critical ροές ανά εβδομάδα (από Πέμπτη 00:00 έως Τετάρτη 23:59)
Επίσης, θα ήθελα αν γίνεται και το πλήθος των καθυστερήσεων για το ίδιο χρονικό διάστημα.

Τα παραπάνω θα μπορούσα να τα τρέχω εγώ στη βάση κάθε Πέμπτη πρωί για την προηγούμενη εβδομάδα ή να έρχεται report. 

Ευχαριστώ εκ των προτέρων,


Με εκτίμηση,
Μαμαλιού Ελένη

*********************************************************************************************/


------------------------------------------------------------------------------------------
--  A.   Για τα errors
--.      Ένα error ορίζεται μοναδικά από τις επόμενες 2  κολώνες: (ERROR_KEY, RTE_SQLERR)
------------------------------------------------------------------------------------------
    
    with errs4dev
    as (
        select CODE
        from MONITOR_DW.ORAERR4DEV
        where who = 'Dev'
    ),
    flow_errs4dev
    as(
        SELECT 
            'DEV_STANDBY_BI' WHO2CALL,    
            ERROR_KEY,
            rta_iid,
            rta_type,
            rta_lob_name,
            rta_primary_target,
            creation_date,
            rte_statement,
            rte_sqlerr,
            rte_sqlerrm,
            main_flow,
            subflows
        FROM stage_dw.dwh_error_table, errs4dev
       WHERE 
            CREATION_DATE >= &Tlast
            AND rte_sqlerrm like '%'||errs4dev.code||'%'
        ORDER BY rta_iid, creation_date DESC
    ),
    flow_errs4as
    as(
        SELECT 
            'AS_STANDBY_BI' WHO2CALL,    
            ERROR_KEY,
            rta_iid,
            rta_type,
            rta_lob_name,
            rta_primary_target,
            creation_date,
            rte_statement,
            rte_sqlerr,
            rte_sqlerrm,
            main_flow,
            subflows
        FROM stage_dw.dwh_error_table
        WHERE 
            CREATION_DATE >= &Tlast
            AND (ERROR_KEY, RTE_SQLERR) NOT IN (select ERROR_KEY, RTE_SQLERR from flow_errs4dev)
        ORDER BY rta_iid, creation_date DESC
    )
    select * from flow_errs4dev
    union
    select * from flow_errs4as        
    ORDER BY 7 DESC;


---------------------- VIEWS -----------------
/*
    All flow errors of the last 7 days
*/
create or replace view MONITOR_DW.V_ALL_FLOW_ERRORS_7DAYS
as
with errs4dev
    as (
        select CODE
        from MONITOR_DW.ORAERR4DEV
        where who = 'Dev'
    ),
    flow_errs4dev
    as(
        SELECT 
            'DEV_STANDBY_BI' WHO2CALL,    
            ERROR_KEY,
            rta_iid,
            rta_type,
            rta_lob_name,
            rta_primary_target,
            creation_date,
            rte_statement,
            rte_sqlerr,
            rte_sqlerrm,
            main_flow,
            subflows
        FROM stage_dw.dwh_error_table, errs4dev
       WHERE 
            CREATION_DATE >= sysdate - 7
            AND rte_sqlerrm like '%'||errs4dev.code||'%'
        ORDER BY rta_iid, creation_date DESC
    ),
    flow_errs4as
    as(
        SELECT 
            'AS_STANDBY_BI' WHO2CALL,    
            ERROR_KEY,
            rta_iid,
            rta_type,
            rta_lob_name,
            rta_primary_target,
            creation_date,
            rte_statement,
            rte_sqlerr,
            rte_sqlerrm,
            main_flow,
            subflows
        FROM stage_dw.dwh_error_table
        WHERE 
            CREATION_DATE >= sysdate - 7
            AND (ERROR_KEY, RTE_SQLERR) NOT IN (select ERROR_KEY, RTE_SQLERR from flow_errs4dev)
        ORDER BY rta_iid, creation_date DESC
    )
    select * from flow_errs4dev
    union
    select * from flow_errs4as        
    ORDER BY 7 DESC;

/*
    Critical (only) Flow errors of the last 7 days
*/

create or replace view MONITOR_DW.V_CRITICAL_FLOW_ERRORS_7DAYS
as
select t1.*
from MONITOR_DW.V_ALL_FLOW_ERRORS_7DAYS t1 join MONITOR_DW.DWP_ETL_FLOWS t2 on ( t1.main_flow = T2.FLOW_NAME)
where
    T2.CRITICAL_IND = 1;
    
    
/*
    το πλήθος των errors στις critical ροές ανά εβδομάδα (από Πέμπτη 00:00 έως Τετάρτη 23:59)
*/   
create or replace view monitor_dw.v_crtcl_flow_errs_7days_stats
as 
select main_flow, count(distinct ERROR_KEY||RTE_SQLERR) num_of_errors
from MONITOR_DW.V_CRITICAL_FLOW_ERRORS_7DAYS
where
    CREATION_DATE < trunc(sysdate)
group by main_flow    
order by 1
    

grant select on STAGE_DW.DWH_ERROR_TABLE to monitor_dw with grant option;
grant select on monitor_dw.v_crtcl_flow_errs_7days_stats to EMAMALIOU;


select *
from  monitor_dw.v_crtcl_flow_errs_7days_stats

------------------------------------------------------------------------------------------
-- B.   Για τα errors
--.     Ένα error ορίζεται μοναδικά από τις επόμενες 2  κολώνες: ("Problem / Action", nvl(to_char(OWB_AUDIT_ID), SQL_ID))
------------------------------------------------------------------------------------------   

-- Για τις καθυστερήσεις

SELECT SNAPSHOT_DT,
       "Problem / Action",
       OWB_FLOW,
       USERNAME,
       OWB_NAME,
       OWB_TYPE,
       OWB_AUDIT_ID,
       OWB_CREATED_ON,
       OWB_UPDATED_ON,
       OWB_STATUS,
       OWB_RESULT,
       DURATION_MINS,
       OWB_DURATION_MINS_P80,
       OWB_TIMES_EXCEEDING_P80,
       INST_ID,
       SID,
       SERIAL#,
       LOGON_TIME,
       STATUS,
       PROG,
       SQL_ID,
       SQL_CHILD_NUMBER,
       SQL_EXEC_START,
       PLAN_HASH_VALUE,
       SQL_TEXT,
       ENTRY_PLSQL_PROC,
       WAIT_STATE,
       WAIT_CLASS,
       EVENT,
       SECS_IN_WAIT,
       OBJ_OWNER,
       OBJ_NAME,
       OBJ_TYPE,
       BLOCKING_INSTANCE,
       BLOCKING_SESSION,
       BLOCKER,
       BLOCKER_OWB_NODE,
       BLOCKER_MAIN_FLOW,
       KILL_BLOCKER_STMNT,
       OSUSER,
       OSPROCESS,
       MACHINE,
       PORT,
       TERMINAL,
       PREV_SQL_ID,
       PREV_CHILD_NUMBER,
       PREV_EXEC_START,
       PREV_PLAN_HASH_VALUE,
       PREV_SQL_TEXT
from MONITOR_DW.FSESS_OWB
where
    snapshot_dt >= &Tlast
     
------------------------------ VIEWS --------------------
/*
    All flow delays of the last 7 days
*/

create or replace view MONITOR_DW.V_ALL_FLOW_DELAYS_7DAYS
as
SELECT SNAPSHOT_DT,
       "Problem / Action",
       OWB_FLOW,
       USERNAME,
       OWB_NAME,
       OWB_TYPE,
       OWB_AUDIT_ID,
       OWB_CREATED_ON,
       OWB_UPDATED_ON,
       OWB_STATUS,
       OWB_RESULT,
       DURATION_MINS,
       OWB_DURATION_MINS_P80,
       OWB_TIMES_EXCEEDING_P80,
       INST_ID,
       SID,
       SERIAL#,
       LOGON_TIME,
       STATUS,
       PROG,
       SQL_ID,
       SQL_CHILD_NUMBER,
       SQL_EXEC_START,
       PLAN_HASH_VALUE,
       SQL_TEXT,
       ENTRY_PLSQL_PROC,
       WAIT_STATE,
       WAIT_CLASS,
       EVENT,
       SECS_IN_WAIT,
       OBJ_OWNER,
       OBJ_NAME,
       OBJ_TYPE,
       BLOCKING_INSTANCE,
       BLOCKING_SESSION,
       BLOCKER,
       BLOCKER_OWB_NODE,
       BLOCKER_MAIN_FLOW,
       KILL_BLOCKER_STMNT,
       OSUSER,
       OSPROCESS,
       MACHINE,
       PORT,
       TERMINAL,
       PREV_SQL_ID,
       PREV_CHILD_NUMBER,
       PREV_EXEC_START,
       PREV_PLAN_HASH_VALUE,
       PREV_SQL_TEXT
from MONITOR_DW.FSESS_OWB
where
    snapshot_dt >= sysdate - 7;
    
/*
    Critical (only) Flow delays of the last 7 days
*/

create or replace view MONITOR_DW.V_CRITICAL_FLOW_DELAYS_7DAYS
as
select t1.*
from MONITOR_DW.V_ALL_FLOW_DELAYS_7DAYS t1 join MONITOR_DW.DWP_ETL_FLOWS t2 on ( t1.owb_flow = T2.FLOW_NAME)
where
    T2.CRITICAL_IND = 1;
    
/*
    το πλήθος των delays στις critical ροές ανά εβδομάδα (από Πέμπτη 00:00 έως Τετάρτη 23:59)
*/   
create or replace view monitor_dw.v_crtcl_flow_dlays_7days_stats
as 
select owb_flow, count(distinct "Problem / Action"||nvl(to_char(OWB_AUDIT_ID), SQL_ID)) num_of_delays
from MONITOR_DW.V_CRITICAL_FLOW_DELAYS_7DAYS
where
    snapshot_dt < trunc(sysdate)
group by owb_flow    
order by 1
    

grant select on monitor_dw.v_crtcl_flow_dlays_7days_stats to EMAMALIOU;


select *
from  monitor_dw.v_crtcl_flow_dlays_7days_stats;


-- ******** FINAL (OVERALL) VIEW ************

/*
create or replace view monitor_dw.v_crtcl_flow_inc_7days_stats
as 
select  decode(category, 'e', 'Errors','Delays') "Category",
        main_flow "Flow",
        num_of_errors "NumOf"
from (        
select main_flow, num_of_errors, 'e' category
from  monitor_dw.v_crtcl_flow_errs_7days_stats
union all
select owb_flow, num_of_delays, 'd' category
from  monitor_dw.v_crtcl_flow_dlays_7days_stats
)
order by "Flow";    

*/

-- outer join in order to densify the report 
create or replace view monitor_dw.v_crtcl_flow_inc_7days_stats
as 
select  decode(t2.cat, 'e', 'Errors','Delays') "Category",
        flow_name "Flow",
        nvl(num_of_errors, 0) "NumOf"
from    
    (select distinct FLOW_NAME, 'd' cat from MONITOR_DW.DWP_ETL_FLOWS t2 where T2.CRITICAL_IND = 1
     union all
     select distinct FLOW_NAME, 'e' cat from MONITOR_DW.DWP_ETL_FLOWS t2 where T2.CRITICAL_IND = 1) t2
    left outer join
     (        
        select main_flow, num_of_errors, 'e' category
        from  monitor_dw.v_crtcl_flow_errs_7days_stats
        --where main_flow = 'PER_MAIN'
        union all
        select owb_flow, num_of_delays, 'd' category
        from  monitor_dw.v_crtcl_flow_dlays_7days_stats
        --where owb_flow = 'PER_MAIN'
    ) t1
 --   PARTITION BY (t1.main_flow)
    on ( t1.main_flow = T2.FLOW_NAME and t1.category = t2.cat) 
order by  flow_name   
    

grant select on monitor_dw.v_crtcl_flow_inc_7days_stats to EMAMALIOU;

revoke select on monitor_dw.v_crtcl_flow_inc_7days_stats from EMAMALIOU;

grant select on monitor_dw.v_crtcl_flow_inc_7days_stats to oper_user;

select * from monitor_dw.v_crtcl_flow_inc_7days_stats;



------------------------- DRAFT ------------------------------------------
select date'2015-11-05' - 7 from dual

select *
from dba_users
where username like '%MAMA%'


select  decode(category, 'e', 'Errors','Delays') "Category",
        main_flow "Flow",
        num_of_errors "NumOf"
from (        
    select main_flow, num_of_errors, 'e' category
    from  monitor_dw.v_crtcl_flow_errs_7days_stats
    union all
    select owb_flow, num_of_delays, 'd' category
    from  monitor_dw.v_crtcl_flow_dlays_7days_stats
    ) t1
    PARTITION BY (t1.category, t1.main_flow)
    right outer join 
    (select distinct FLOW_NAME from MONITOR_DW.DWP_ETL_FLOWS t2 where T2.CRITICAL_IND = 1) t2
    on ( t1.main_flow = T2.FLOW_NAME) 
order by "Flow";  

