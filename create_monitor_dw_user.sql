/***************************************************
create_enviroinment version 1.0

Script to create enviroinment for DWH monitoring

last update: 14/06/2008
author: nkarag
****************************************************/

/**
Create a user for DWH monitoring
*/

CREATE USER "MONITOR_DW" IDENTIFIED BY "MONITOR_DW"
 DEFAULT TABLESPACE USERS
 TEMPORARY TABLESPACE TEMP
 --QUOTA UNLIMITED ON USERS
 --QUOTA UNLIMITED ON TEMP;
/

grant dba to monitor_dw
/
alter user monitor_dw identified by "ot3mdw"
/

/**
Need to explicitly grant access to dictionary tables because this is granted through the DBA role
and Roles are disabled within Procedures and hence it needs to be granted directly, in order to view these tables
from within a PL/SQL procedure
*/
grant select any dictionary to monitor_dw
/

grant create any view to monitor_dw
/

grant select any table to monitor_dw
/
grant create any table to monitor_dw
/
