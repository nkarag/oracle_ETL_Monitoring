-- create function-based index to allow searches by date
drop index monitor_dw.VSQLSTATS_SNAP_IDX1;
create index monitor_dw.VSQLSTATS_SNAP_IDX1 on monitor_dw.MONDW_VSQLSTATS_SNAP(trunc(population_date)) 
parallel 16
compute statistics;

-- create function-based index to allow searches by date
drop index MONITOR_DW.MONDW_VSESSION_SNAP_IDX1;
create index MONITOR_DW.MONDW_VSESSION_SNAP_IDX1 on MONITOR_DW.MONDW_VSESSION_SNAP (trunc(population_date)) 
parallel 16
compute statistics; 

exec dbms_stats.gather_table_stats('MONITOR_DW','MONDW_VSQLSTATS_SNAP');

exec dbms_stats.gather_table_stats('MONITOR_DW','MONDW_VSESSION_SNAP'); 