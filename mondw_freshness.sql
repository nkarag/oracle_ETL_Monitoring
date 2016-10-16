create table mondw_freshness as
select sysdate as snapshot_datetime 
    ,(select RUN_DATE from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'OTE_DW_LAST_RUN') OTE_DW_LAST_RUN
    ,(select RUN_DATE from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'RATE_DATE') RATE_DATE
    , trunc(sysdate) - (select trunc(RUN_DATE) from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'OTE_DW_LAST_RUN') as other_facts_days_diff
    ,trunc(sysdate) - (select trunc(RUN_DATE) from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'RATE_DATE') as cdrs_days_diff
from dual 


insert into mondw_freshness 
select sysdate as snapshot_datetime 
    ,(select RUN_DATE from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'OTE_DW_LAST_RUN') OTE_DW_LAST_RUN
    ,(select RUN_DATE from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'RATE_DATE') RATE_DATE
    , trunc(sysdate) - (select trunc(RUN_DATE) from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'OTE_DW_LAST_RUN') as other_facts_days_diff
    ,trunc(sysdate) - (select trunc(RUN_DATE) from STAGE_DW.DW_CONTROL_TABLE where PROCEDURE_NAME = 'RATE_DATE') as cdrs_days_diff
from dual;

commit; 

