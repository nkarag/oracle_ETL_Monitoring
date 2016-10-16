/*
query performance KPIs (since 23/6/2008)
*/
-- average/max/min number of selects per day
SELECT avg(nofsel), min(nofsel), max(nofsel)
FROM (
select DAY, sum(NUM_OF_SELECTS_EXECUTED) nofsel 
from MONITOR_DW.V_NUM_OF_SQLS_PER_USER_PER_DAY
where day > to_date('01/01/2009', 'dd/mm/yyyy')
GROUP BY DAY
) t

-- num of queries by day - evolution
select DAY, sum(NUM_OF_SELECTS_EXECUTED) nofsel 
from MONITOR_DW.V_NUM_OF_SQLS_PER_USER_PER_DAY
GROUP BY DAY
order by 1 asc
     

-- average/min/max bo executions by day
SELECT avg(nofboex), min(nofboex), max(nofboex)
FROM (
select DAY, sum(NUM_OF_BO_EXECTNS) nofboex 
from MONITOR_DW.V_NUMOF_BO_EXECTNS_PUSERPDAY 
where day > to_date('01/01/2009', 'dd/mm/yyyy')
GROUP BY DAY
) t

-- num of BO executions - evolution
select DAY, sum(NUM_OF_BO_EXECTNS) nofboex 
from MONITOR_DW.V_NUMOF_BO_EXECTNS_PUSERPDAY 
GROUP BY DAY
order by 1 asc
 
-- Percent of queries within elapsed-time bands
        select  case 
                    when  ELAPSED_TIME_MINS < 10 then 'lt_10'
                    when  ELAPSED_TIME_MINS between 10 and 30   then 'between_10-30'
                    when  ELAPSED_TIME_MINS between 31 and 60   then 'between_31-60'
                    when  ELAPSED_TIME_MINS between 61 and 120   then 'between_61-120'
                    else 'gt_120'             
                end as elpsd_time_band
                ,sft.*
        from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT sft 
        
select  elpsd_time_band
        ,count(*) as num_of_selects_per_band
        ,count(*)/max(total_num_of_selects) as prcnt_of_total_queries               
from (
        select  case 
                    when  ELAPSED_TIME_MINS < 5 then 'lt_5'
                    when  ELAPSED_TIME_MINS between 6 and 10   then 'between_06-10'
                    when  ELAPSED_TIME_MINS between 10 and 30   then 'between_10-30'
                    when  ELAPSED_TIME_MINS between 31 and 60   then 'between_31-60'
                    when  ELAPSED_TIME_MINS between 61 and 120   then 'between_61-120'
                    else 'gt_120'             
                end as elpsd_time_band
                ,count(*) over() as total_num_of_selects
                ,sft.*
        from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT sft 
) t1        
group by  elpsd_time_band
order by 2 desc


-- Percent of BO actions within elapsed-time bands
select  elpsd_time_band
        ,count(*) as num_of_selects_per_band
        ,count(*)/max(total_num_of_selects) as prcnt_of_total_queries               
from (
        select  case 
                    when  DURATION_IN_MINS < 5 then 'lt_5'
                    when  DURATION_IN_MINS between 6 and 10   then 'between_06-10'
                    when  DURATION_IN_MINS between 10 and 30   then 'between_10-30'
                    when  DURATION_IN_MINS between 31 and 60   then 'between_31-60'
                    when  DURATION_IN_MINS between 61 and 120   then 'between_61-120'
                    else 'gt_120'             
                end as elpsd_time_band
                ,count(*) over() as total_num_of_selects
                ,sft.*
        from MONITOR_DW.V_USER_BO_REPORT_EXECTNS_FCT sft 
) t1        
group by  elpsd_time_band
order by 2 desc

-- number of concurrent active sessions (estimate!)
select num_concur_sess, count(*) frequency 
from
(
-- Number of concurrent active database sessions (for selects only without parallelism) multiplied by the table degree of parallism
select population_date, count(*)*24 as num_concur_sess
from MONITOR_DW.MONDW_VSESSION_SNAP
group by population_date 
--order by 2 desc
) t
group by num_concur_sess
order by 2 desc 

-- monitor parallel slaves
SELECT px.qcsid,S.SID,s.serial#,s.status,substr(sql_text,1,100),OSUSER,s.USERNAME,S.TERMINAL,MACHINE,S.SERIAL#,P.SPID,px.req_degree,px.degree, 
ROWS_PROCESSED,DISK_READS,BUFFER_GETS 
FROM V$SESSION S,V$SQLAREA A ,V$PROCESS P, v$px_session px 
WHERE A.ADDRESS=S.SQL_ADDRESS 
AND A.HASH_VALUE=S.SQL_HASH_VALUE 
AND P.ADDR=S.PADDR 
and px.sid=s.sid 
AND STATUS='ACTIVE' 
--and px.qcsid=1882 
order by 1,2 

-- Execution time for main flows
-- level0 daily
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration > 10 then 'gt_10'
                        else to_char(duration)             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('LEVEL0_DAILY')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
and   A.CREATED_ON>to_date('08/04/2009','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
        ,   case 
                        when  duration > 10 then 'gt_10'
                        else to_char(duration)             
                    end 
order by 4 desc

-- CDRs daily
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration > 10 then 'gt_10'
                        else to_char(duration)             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME like 'DAILY_USAGE_PF'--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
--and   A.CREATED_ON>to_date('01/12/2008','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
        ,   case 
                        when  duration > 10 then 'gt_10'
                        else to_char(duration)             
                    end 
order by 4 desc

/*
select 
    case 
                        when  hrs < 5 then 'lt_5'
                        when  hrs between 5 and 10   then 'between_05-10'
                        else 'gt_10'             
                    end as elpsd_time_band
    ,sum(frequency) as freq                     
from
(
select EXECUTION_NAME
        , duration as hrs
        , count(*) as frequency
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt    
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('DAILY_USAGE_PF')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
--and   A.CREATED_ON>to_date('01/12/2008','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME, duration
order by  2 asc
) t
group by    case 
                        when  hrs < 5 then 'lt_5'
                        when  hrs between 5 and 10   then 'between_05-10'
                        else 'gt_10'             
                    end 
*/                    

-- CDRs extraction
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration > 10 then 'gt_10'
                        else to_char(duration)             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('USAGE_EXTRACTION_PF')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
--and   A.CREATED_ON>to_date('01/12/2008','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
        ,   case 
                        when  duration > 10 then 'gt_10'
                        else to_char(duration)             
                    end 
order by 4 desc

/*
select 
    case 
                        when  hrs < 5 then 'lt_5'
                        when  hrs between 5 and 10   then 'between_05-10'
                        else 'gt_10'             
                    end as elpsd_time_band
    ,sum(frequency) as freq                     
from
(
select EXECUTION_NAME
        , duration as hrs
        , count(*) as frequency
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('USAGE_EXTRACTION_PF')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
--and   A.CREATED_ON>to_date('01/12/2008','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME, duration
order by  2 asc
) t
group by    case 
                        when  hrs < 5 then 'lt_5'
                        when  hrs between 5 and 10   then 'between_05-10'
                        else 'gt_10'             
                    end 
*/

-- perif_main
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration > 240 then 'gt_240'
                        when duration between 121 and 240 then 'between_121_240'
                        when duration between 60 and 120 then 'between_60_120'
                        when duration = 0 then '0'
                        else 'lt_60'             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24*60) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('PERIF_MAIN')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
and   A.CREATED_ON>to_date('01/01/2009','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
,   case 
                        when  duration > 240 then 'gt_240'
                        when duration between 121 and 240 then 'between_121_240'
                        when duration between 60 and 120 then 'between_60_120'
                        when duration = 0 then '0'
                        else 'lt_60'             
                    end
order by 4 desc

/*
select EXECUTION_NAME, duration as hrs, count(*) as frequency
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('PERIF_MAIN')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
--and   A.CREATED_ON>to_date('01/12/2008','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME, duration
order by  2 asc
*/

-- Execution time for truncates
-- AFTER the patch
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration < 10 then 'lt_10'
                        when  duration between 10 and 20 then 'between_10-20'
                        when  duration between 21 and 30 then 'between_21-30'
                        when  duration > 30 then 'gt_30'
                        else to_char(duration)             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24*60) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME like '%TRUNCATE_ORDER_DETAIL%' --, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
and   A.CREATED_ON>to_date('26/04/2009','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
       ,   case 
                        when  duration < 10 then 'lt_10'
                        when  duration between 10 and 20 then 'between_10-20'
                        when  duration between 21 and 30 then 'between_21-30'
                        when  duration > 30 then 'gt_30'
                        else to_char(duration)             
                    end 
order by 4 desc

-- Before the patch
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration < 10 then 'lt_10'
                        when  duration between 10 and 20 then 'between_10-20'
                        when  duration between 21 and 30 then 'between_21-30'
                        when  duration > 30 then 'gt_30'
                        else to_char(duration)             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24*60) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME like '%TRUNCATE_ORDER_DETAIL%' --, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
and   A.CREATED_ON < to_date('26/04/2009','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
       ,   case 
                        when  duration < 10 then 'lt_10'
                        when  duration between 10 and 20 then 'between_10-20'
                        when  duration between 21 and 30 then 'between_21-30'
                        when  duration > 30 then 'gt_30'
                        else to_char(duration)             
                    end 
order by 4 desc