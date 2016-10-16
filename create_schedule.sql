BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM(
program_name=>'MONITOR_DW.DWMON_PERIODIC_VSESSION_SNAP',
program_action=>'begin
dwmon_package.DWMON_PERIODIC_VSESSION_SNAP;
end;',
program_type=>'PLSQL_BLOCK',
number_of_arguments=>0,
comments=>'call dwmon_package.DWMON_PERIODIC_VSESSION_SNAP',
enabled=>TRUE);
END;
/

BEGIN
sys.dbms_scheduler.create_schedule( 
repeat_interval => 'FREQ=MINUTELY;INTERVAL=10',
start_date => to_timestamp_tz('2008-06-24 07:00:00 +3:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'),
comments => 'call DWMON_PERIODIC_VSESSION_SNAP every 10 min',
schedule_name => '"MONITOR_DW"."DWMON_PERIODIC_VSESSION_SNAP"');
END;

BEGIN
sys.dbms_scheduler.create_job( 
job_name => '"MONITOR_DW"."DWMON_VSESSION_SNAP_JOB"',
program_name => 'MONITOR_DW.DWMON_PERIODIC_VSESSION_SNAP',
schedule_name => 'MONITOR_DW.DWMON_VSESSION_SNP_10MIN',
job_class => 'DEFAULT_JOB_CLASS',
comments => 'call DWMON_PERIODIC_VSESSION_SNAP every 10 min',
auto_drop => FALSE,
enabled => FALSE);
sys.dbms_scheduler.set_attribute( name => '"MONITOR_DW"."DWMON_VSESSION_SNAP_JOB"', attribute => 'logging_level', value => DBMS_SCHEDULER.LOGGING_OFF); 
sys.dbms_scheduler.enable( '"MONITOR_DW"."DWMON_VSESSION_SNAP_JOB"' ); 
END;


-- allagi sto program 27/6/2008 -- na kalei thn dwmon_main_gather_usg_stats anti tin  DWMON_PERIODIC_VSESSION_SNAP

/

BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM(
program_name=>'MONITOR_DW.dwmon_mondw_users_update',
program_action=>'begin
dwmon_mondw_users_update;
end;',
program_type=>'PLSQL_BLOCK',
number_of_arguments=>0,
comments=>'periodic update of mondw_users table from dba_tables',
enabled=>TRUE);
END;

BEGIN
sys.dbms_scheduler.create_schedule( 
repeat_interval => 'FREQ=DAILY',
start_date => systimestamp at time zone '+3:00',
comments => 'call dwmon_mondw_users_update every day',
schedule_name => '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_DAILY"');
END;

BEGIN
sys.dbms_scheduler.create_job( 
job_name => '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_JOB"',
program_name => 'MONITOR_DW.DWMON_MONDW_USERS_UPDATE',
schedule_name => 'MONITOR_DW.DWMON_MONDW_USERS_UPDATE_DAILY',
job_class => 'DEFAULT_JOB_CLASS',
comments => 'call DWMON_PERIODIC_VSESSION_SNAP every 10 min',
auto_drop => FALSE,
enabled => FALSE);
sys.dbms_scheduler.set_attribute( name => '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_JOB"', attribute => 'logging_level', value => DBMS_SCHEDULER.LOGGING_OFF); 
sys.dbms_scheduler.set_attribute( name => '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_JOB"', attribute => 'job_weight', value => 1); 
sys.dbms_scheduler.enable( '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_JOB"' ); 
END;

BEGIN
sys.dbms_scheduler.set_attribute( name => '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_DAILY"', attribute => 'repeat_interval', value => 'FREQ=DAILY;BYHOUR=20;BYMINUTE=0;BYSECOND=0'); 
END;

BEGIN
sys.dbms_scheduler.disable( '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_JOB"' ); 
sys.dbms_scheduler.set_attribute( name => '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_JOB"', attribute => 'comments', value => 'call MONITOR_DW.DWMON_MONDW_USERS_UPDATE every day'); 
sys.dbms_scheduler.enable( '"MONITOR_DW"."DWMON_MONDW_USERS_UPDATE_JOB"' ); 
END;

BEGIN
DBMS_SCHEDULER.DISABLE(
name=>'MONITOR_DW.DWMON_MONDW_USERS_UPDATE',
force=>TRUE);
END;

BEGIN
DBMS_SCHEDULER.SET_ATTRIBUTE(
name=>'MONITOR_DW.DWMON_MONDW_USERS_UPDATE',
attribute=>'PROGRAM_ACTION',
value=>'begin
dwmon_package.dwmon_mondw_users_update;
end;    ');
END;

BEGIN
DBMS_SCHEDULER.ENABLE(
name=>'MONITOR_DW.DWMON_MONDW_USERS_UPDATE');
END;


BEGIN
sys.dbms_scheduler.create_schedule( 
repeat_interval => 'FREQ=DAILY;BYHOUR=9;BYMINUTE=0;BYSECOND=0',
start_date => to_timestamp_tz('2008-10-18 +3:00', 'YYYY-MM-DD TZH:TZM'),
comments => 'everyday logging od freshness of the data',
schedule_name => '"MONITOR_DW"."DWMON_LOG_FRESHNESS_OF_DATA"');
END;


BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM(
program_name=>'MONITOR_DW.DWMON_LOG_FRESH_OF_DATA_PROG',
program_action=>'begin
dwmon_package.dwmon_log_freshness_of_data;
end;',
program_type=>'PLSQL_BLOCK',
number_of_arguments=>0,
comments=>'call routine to log frshness of the data',
enabled=>FALSE);
END;

BEGIN
sys.dbms_scheduler.create_job( 
job_name => '"MONITOR_DW"."DWMON_LOG_FRESH_OF_DATA_JOB"',
program_name => 'MONITOR_DW.DWMON_LOG_FRESH_OF_DATA_PROG',
schedule_name => 'MONITOR_DW.DWMON_LOG_FRESHNESS_OF_DATA',
job_class => 'DEFAULT_JOB_CLASS',
comments => 'job to log the freshness of data every day',
auto_drop => FALSE,
enabled => TRUE);
END;







