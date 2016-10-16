/*
What is the query load makeup? (Rough estimation - Batch does not include daily ETL)
 % Ad-Hoc (user-written SQL)     % canned reports   % Ad-Hoc via a tool  % Batch jobs
*/

-- percent of selects, percent of non-select commands
select
sum(case when command = 3 then cnt else null end) cnt_sel,
to_char(sum(case when command = 3 then cnt else null end)/total, '9.99'),
sum(case when command <> 3 then cnt else null end) cnt_non_sel,
to_char(sum(case when command <> 3 then cnt else null end)/total, '9.99'),
total
from
(
    select t.command, t2.name, t.program, count(t.sql_id) cnt, t.total, to_char( count(t.sql_id)/t.total, '9.99999') prcnt 
    from
    (
        select sql_id, a.command, a.program,  count(a.sql_id) over() total
        from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT a
    )t left outer join audit_actions t2 on (t.command = t2.ACTION)
    group by t.command, t2.name, t.program, t.total
    --order by 1,2
)t3
where name is not null -- exclude unknown commands
group by total


select * from audit_actions

select command, count(*)
from MONITOR_DW.MONDW_VSESSION_SNAP
group by command 

select *
from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT 
where command = -86

/*
How do you control the degree of parallelism?
*/
select degree, count(*)
from dba_tables
group by degree


select *
from stage_dw.dw_control_table


/*
Breakdown the time spent on ETL?
____ % Extraction 	____ % Transformation 	____ % Load

1. USAGE_EXTRACTION_PF --> all extraction
2. DAILY_USAGE_PF --> Transformation + loading
3. LEVEL0_DAILY --> Extraction _ Transf. + Loading
*/

-- 1.
-- CDRs extraction 


-- NOTE: In the following query CDR Extraction alos includes DAILY_USAGE_PF (CDR Processing) and thus the total time is increased.
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration > 20 then 'gt_20'
                        else to_char(duration)             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME in ('USAGE_EXTRACTION_PF')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
and   A.CREATED_ON>to_date('01/03/2010','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
           ,case 
                        when  duration > 20 then 'gt_20'
                        else to_char(duration)             
                    end 
order by 4 desc



/*
  Get the average net extraction time for a single rate date from USAGE_EXTRACTION_PF
  Exclude the cases where it is run in catch up mode (i.e. running continously) - Assumption: < 11 hours
*/
select sum(duration_time_band*frequency)/sum(frequency) 
from (
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
            from owb.all_rt_audit_executions a 
            where a.EXECUTION_NAME = 'USAGE_EXTRACTION_PF:DAILY_USAGE_PF'--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/01/2010','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            --and PARENT_AUDIT_EXECUTION_ID is null 
            ORDER BY A.CREATED_ON DESC
        ) t1 join
        ( -- execution times of USAGE_EXTRACTION_PF
            select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
            from owb.all_rt_audit_executions a 
            where a.EXECUTION_NAME in ('USAGE_EXTRACTION_PF')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/01/2010','dd/mm/yyyy') 
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
)
where
frequency > 1 -- exclude outliers and special cases
and duration_time_band > 0 and duration_time_band <11


-- 2.

-- CDRs daily processing

-- NOTE: in the following query DAILY_USAGE_PF is included in USAGE_EXTRACTION_PF and thus is called by this parent flow. However sometimes is called on each own
select EXECUTION_NAME 
       --,duration as hrs
       ,   case 
                        when  duration > 20 then 'gt_20'
                        else to_char(duration)             
                    end as duration_time_band
       ,count(*) as frequency
       ,count(*) / max(tot_cnt) as prcnt 
from (
select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
from owb.all_rt_audit_executions a 
where a.EXECUTION_NAME like '%DAILY_USAGE_PF'--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
and   A.CREATED_ON>to_date('01/01/2010','dd/mm/yyyy') 
and a.execution_audit_status='COMPLETE' and a.return_result='OK'
--and PARENT_AUDIT_EXECUTION_ID is null 
ORDER BY A.CREATED_ON DESC
)
group by EXECUTION_NAME
        --, duration
        ,   case 
                        when  duration > 20 then 'gt_20'
                        else to_char(duration)             
                    end 
order by 4 desc

/*
  Get the average net tranformation time for a single rate date from DAILY_USAGE_PF
*/
select sum(duration_time_band*frequency)/sum(frequency) 
from (
    /*
      Get the duration of the net tranformation time for a single rate date from DAILY_USAGE_PF
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
        select t1.EXECUTION_NAME, t1.CREATED_ON, t2.CREATED_ON, t2.UPDATED_ON,  trunc((t2.created_on - t1.created_on)*24) duration, count(*) over() as tot_cnt 
        from
        ( -- execution times of DAILY_USAGE_PF, , in the current year (tranformatioin + loading)
            select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON,  trunc((updated_on-created_on)*24) duration, count(*) over() as tot_cnt  
            from owb.all_rt_audit_executions a 
            where a.EXECUTION_NAME like '%DAILY_USAGE_PF'--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/01/2010','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            --and PARENT_AUDIT_EXECUTION_ID is null 
            ORDER BY A.CREATED_ON DESC
        ) t1 join
        ( -- execution times of DAILY_USAGE_PF (loading only)
            select a.EXECUTION_NAME, a.CREATED_ON,  b.UPDATED_ON,  trunc((b.updated_on-a.created_on)*24) duration, count(*) over() as tot_cnt  
            from owb.all_rt_audit_executions a, owb.all_rt_audit_executions b  
            where a.EXECUTION_NAME in ('DAILY_USAGE_PF:AND1_1')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/01/2010','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            and a.TOP_LEVEL_EXECUTION_AUDIT_ID = b.EXECUTION_AUDIT_ID
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
)
where
frequency > 1 -- exclude outliers and special cases
and duration_time_band > 0 and duration_time_band <11


/*
  Get the average net loading time for a single rate date from DAILY_USAGE_PF
*/
select sum(duration_time_band*frequency)/sum(frequency) 
from (
    /*
      Get the duration of the net loading time for a single rate date from DAILY_USAGE_PF
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
-- -- execution times of DAILY_USAGE_PF (loading only)
            select a.EXECUTION_NAME, a.CREATED_ON,  b.UPDATED_ON,  trunc((b.updated_on-a.created_on)*24) duration, count(*) over() as tot_cnt  
            from owb.all_rt_audit_executions a, owb.all_rt_audit_executions b  
            where a.EXECUTION_NAME in ('DAILY_USAGE_PF:AND1_1')--, 'DAILY_USAGE_PF', 'USAGE_EXTRACTION_PF', 'PERIF_MAIN') --('LEVEL0_DAILY_WITHOUT_BILLS') --('LEVEL1_FCT_BILLS_1EXTR_SUMMARY')----('ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP1','ORDERS_FCT_SKAUX_PF:ORDER_DW_STEP3' ) --= 'ORDERS_FCT_TARGET_PF:ORDERS_FCT_TARGET'-- -- like 'ORDERS_FCT_SKAUX_PF%' -- 
            and   A.CREATED_ON>to_date('01/01/2010','dd/mm/yyyy') 
            and a.execution_audit_status='COMPLETE' and a.return_result='OK'
            and a.TOP_LEVEL_EXECUTION_AUDIT_ID = b.EXECUTION_AUDIT_ID
            --and PARENT_AUDIT_EXECUTION_ID is null 
            ORDER BY A.CREATED_ON DESC
          )
    group by EXECUTION_NAME
            --, duration
               ,case 
                            when  duration > 20 then -20
                            else duration             
                        end 
    order by 4 desc
)
where
frequency > 1 -- exclude outliers and special cases
and duration_time_band > -1 and duration_time_band <11

-- 3. LEVEL0_DAILY --> Extraction _ Transf. + Loading

-- extraction
select a.EXECUTION_NAME, a.CREATED_ON,  b.UPDATED_ON,  trunc((a.created_on-b.created_on)*24) duration, count(*) over() as tot_cnt
from
(
    select * from owb.all_rt_audit_executions
    where task_name like '%LEVEL0_DAILY:LEVEL1_DIM_3STAGE%'
                and   A.CREATED_ON>to_date('01/01/2010','dd/mm/yyyy') 
            and execution_audit_status='COMPLETE' and return_result='OK'
    order by created_on desc
) a,
(
    select * from owb.all_rt_audit_executions
    where task_name = 'LEVEL0_DAILY'  
            and execution_audit_status='COMPLETE' and return_result='OK'
    order by created_on desc
) b
where a.top_level_audit_execution_id = b.audit_execution_id
order by b.created_on desc, a.created_on

 
/*
extraction 2.15 (2 hours)

stage-shadow: 3.15 (1 hour) ektos apo orders 4.30 (2 hours)

target: 5.15, (1 hour)

basic_segm (xfer): 6.30 (1 hour)

snp: 7.15 (1 hour)
*/


    

------------------------- SCRATCH

select trunc(sysdate, 'MI')
from dual




