begin
dbms_scheduler.STOP_JOB('DWMON_VSESSION_SNAP_JOB',true);
end;