/**
Package to monitor DWH activity
*/
create or replace
PACKAGE DWMON_PACKAGE  AS

/**
Routine to be called periodically. It captures a snapshot of v$session (only changed rows since previous snapshot).
*/
PROCEDURE dwmon_periodic_vsession_snap;

END DWMON_PACKAGE;
/