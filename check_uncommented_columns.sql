
select owner, round(ratio_to_report(count(column_name)) over() * 100, 1) pcnt
from dba_col_comments 
where comments is null
and owner in (
select schema_name from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1
)
group by owner
order by 2 desc



select owner, round(ratio_to_report(count(table_name)) over() * 100, 1) pcnt
from dba_tab_comments 
where comments is null
and owner in (
select schema_name from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1
)
group by owner
order by 2 desc



