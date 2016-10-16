-- check ALL Siebel interface tables that have a data_ref_date ealrier than yesterday 
select * from dwh_control_table@siebelprod
where
    data_ref_Date < trunc(sysdate) - 1

select trunc(sysdate) - 1 from dual;

-- check SOC interface
select * from dwh_control_table@siebelprod
          where table_name in
          ( SELECT TABLE_NAME 
              FROM STAGE_DW.DW_INTERFACE_TABLES 
             WHERE FLOW_NAME = 'SOC_DW_MAIN'
               AND START_DATE_KEY <= (select run_date from STAGE_DW.dw_control_table where procedure_name='SOC_LAST_RUN')
           )
order by data_ref_date asc                          
           
-- check CMP interface           
select * from dwh_control_table@siebelprod
          where upper(table_name) in
          ( SELECT UPPER(TABLE_NAME)
              FROM STAGE_DW.DW_INTERFACE_TABLES 
             WHERE FLOW_NAME = 'CMP_R'
               AND START_DATE_KEY <= (select run_date from STAGE_DW.dw_control_table where procedure_name='CRM_LAST_RUN'))           
order by data_ref_date asc


select * 
from dwsrc.dwh_log_history@siebelprod 
where table_name   in ('DWH_CUSTOMER_KEY', 'DWH_ACCOUNT_KEY', 'DWH_CONTACT_KEY', 'DWH_CUSTOMER', 'DWH_CONTACT')

select data_ref_date, count(*)
from dwsrc.dwh_log_history@siebelprod
group by data_ref_date
order by 1 desc


select *
FROM STAGE_DW.DW_INTERFACE_TABLES

select * 
from dwsrc.dwh_log_history@siebelprod 
where table_name   in ('DWH_ASSET')


select count(*) 
from dwsrc.dwh_asset@siebelprod 
where
    data_ref_Date = date'2015-06-21'