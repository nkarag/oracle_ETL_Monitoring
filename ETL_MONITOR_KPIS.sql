/*************************

    PCNT that KPIDW_MAIN  finishes before specific time threshold (SLA) e.g., 09:00am 

*************************/

with
depend
as  (
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
        where flow = 'KPIDW_MAIN'--nvl('&&flow_name',flow)
    ORDER BY EXECUTION_DATE DESC, END_EXECUTION DESC
) 
select sum(  case    when to_char(end_execution,'hh24') <= '&&hh24_mi' then 1
                       else 0
               end
        )times_success,
        sum(1) times_all,
        round(sum(  case    when to_char(end_execution,'hh24') <= '&&hh24_mi' /* e.g., 09:00  */ then 1
                            else 0
              end
        ) / sum(1) * 100) pcnt_success
from depend;   


/*************************

    PCNT that CTO_MAIN  finishes before specific time threshold (SLA) e.g., 09:00am 

*************************/

with
depend
as  (
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
        where flow = 'CTO_MAIN' --nvl('&&flow_name',flow)
ORDER BY EXECUTION_DATE DESC, END_EXECUTION DESC
) 
select sum(  case    when to_char(end_execution,'hh24') <= '&&hh24_mi' then 1
                       else 0
              end
        )times_success,
        sum(1) times_all,
        round(sum(  case    when to_char(end_execution,'hh24') <= '&&hh24_mi' /* e.g., 09:00  */ then 1
                            else 0
              end
        ) / sum(1) * 100) pcnt_success
from depend;   



----------- DRAFT -----------------------------

select sysdate, to_char(sysdate,'hh24:mi')
from dual
where to_char(sysdate,'hh24:mi') < '12:45'
