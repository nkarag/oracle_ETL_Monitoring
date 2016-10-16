create or replace view monitor_dw.mis_users
as select * from dba_users where 
trunc(created) = date'2013-01-12' or profile = 'MIS_USER'