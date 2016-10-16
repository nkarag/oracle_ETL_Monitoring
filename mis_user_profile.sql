-- create a profile not for actually limiting resources but only for distinguishing the users
create profile mis_user limit LOGICAL_READS_PER_CALL default;


select *
from dba_profiles
where profile = 'MIS_USER';


select * from monitor_dw.mis_users;

alter user TRANOUDII profile mis_user;

select username from dba_users where profile = 'MIS_USER'


select 'alter user '||username||' profile mis_user;'
from monitor_dw.mis_users;
-- 110 rows





