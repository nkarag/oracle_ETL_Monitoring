select to_char(updated_on, 'YYYY/MM') month,
         sum(ORDERS_DELAY_FLG) 
from (
    select      CASE DAY_FINISH  
                    WHEN 'saturday' then 0  
                    WHEN  'sunday' then  0
                    ELSE    CASE WHEN updated_on > TARGET THEN 1 ELSE 0 END
                  END   ORDERS_DELAY_FLG,
                  CREATED_ON,
                  UPDATED_ON,
                  DAY_FINISH                          
    from (
        select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON, to_timestamp(updated_on)+ interval '11' hour TARGET, trim(to_char(updated_on, 'day')) DAY_FINISH,  round((updated_on-created_on)*24, 1) duration_hours  
        from owb.all_rt_audit_executions a 
        where a.EXECUTION_NAME like '%LEVEL1_FCT_ORD_4TARGET'
        and   A.CREATED_ON>to_date('01/08/2011','dd/mm/yyyy') 
        and a.execution_audit_status='COMPLETE' and a.return_result='OK'
        ORDER BY A.CREATED_ON DESC
    ) t1
    order by CREATED_ON DESC
) t2
group by to_char(updated_on, 'YYYY/MM')
order by 1 desc

---------------------------------------------------- DRAFT ----------------------

select trunc(sysdate,'HH24'),  sysdate, to_timestamp(sysdate)+ interval '11' hour TARGET , extract( HOUR FROM systimestamp), systimestamp, to_timestamp(sysdate) from dual


select to_date('20/12/2011 10:30:25', 'dd/mm/yyyy HH24:MI:SS') from dual


select     case when to_char(to_date('19/11/2011 23:32:49', ' dd/mm/yyyy HH24:MI:SS'), 'day') = 'saturday ' THEN 0
                    when trunc(to_char(to_date('19/11/2011 23:32:49', ' dd/mm/yyyy HH24:MI:SS'), 'day')) = 'sunrday ' THEN 1   
                     ELSE   2  /* CASE WHEN to_date('19/11/2011 23:32:49', ' dd/mm/yyyy HH24:MI:SS') > 
                to_timestamp (to_date('19/11/2011 23:32:49', ' dd/mm/yyyy HH24:MI:SS')) + interval '11' hour THEN 1 ELSE 0 END*/
                END  
from dual

select trim(to_char(to_date('27/11/2011 23:32:49', ' dd/mm/yyyy HH24:MI:SS'), 'day')) from dual


    select a.EXECUTION_NAME, CREATED_ON,  UPDATED_ON, to_timestamp(to_char(updated_on, 'dd/mm/yyyy HH24:MI:SS'), 'dd/mm/yyyy HH24:MI:SS'), to_char(updated_on, 'day') DAY,  round((updated_on-created_on)*24, 1) duration_hours  
    from owb.all_rt_audit_executions a 
    where a.EXECUTION_NAME like '%LEVEL1_FCT_ORD_4TARGET'
    and   A.CREATED_ON>to_date('01/10/2011','dd/mm/yyyy') 
    and a.execution_audit_status='COMPLETE' and a.return_result='OK'
    and rownum < 10