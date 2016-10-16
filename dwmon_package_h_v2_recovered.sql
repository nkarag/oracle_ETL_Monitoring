/******************************

Version 2
New --> 
    routine to capture V$SQLSTATS

Last updated: 26/6/2008
Author: nkarag
*******************************/

/**
Package to monitor DWH activity
*/
create or replace
PACKAGE DWMON_PACKAGE  AS

/**
Routine to be called periodically. It captures a snapshot of v$session (only changed rows since previous snapshot).
*/
PROCEDURE dwmon_periodic_vsession_snap;

/**
Routine to be called periodically. It captures a snapshot of v$sqlstats (only changed rows since previous snapshot).
*/
PROCEDURE dwmon_periodic_vsqlstats_snap;


END DWMON_PACKAGE;
/
