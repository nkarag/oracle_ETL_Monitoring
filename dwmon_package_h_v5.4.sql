/******************************
Version 5.4
    after patch 10.2.0.5 was applied one column has been added into V$SESSION. This caused an ORA-00913: too many values  error in an insert statement where I used
    s.* to refer to all V$SESSION columns. I changed the statement to refer explicitly to the columns of interest
Version 3
New -->
    routine to update mondw_users table
    dwmon_mondw_users_update

Version 2
New --> 
    routine to capture V$SQLSTATS (dwmon_periodic_vsqlstats_snap)
    main routine to call the two statistics capture routines (dwmon_main_gather_usg_stats)

Last updated: 26/6/2008
Author: nkarag
*******************************/

/**
Package to monitor DWH activity
*/
create or replace
PACKAGE monitor_dw.DWMON_PACKAGE  AS

/**
main routine to call the two statistics capture routines
*/
procedure dwmon_main_gather_usg_stats;

/**
Routine to be called periodically. It captures a snapshot of v$session (only changed rows since previous snapshot).
*/
PROCEDURE dwmon_periodic_vsession_snap;

/**
Routine to be called periodically. It captures a snapshot of v$sqlstats (only changed rows since previous snapshot).
*/
PROCEDURE dwmon_periodic_vsqlstats_snap;

Procedure dwmon_mondw_users_update;

Procedure dwmon_log_freshness_of_data;

END DWMON_PACKAGE;
/
