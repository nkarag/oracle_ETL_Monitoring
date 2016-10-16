alter table MONITOR_DW.MONDW_USERS add constraint userspk primary key (user_id)

create index username_idx on MONITOR_DW.MONDW_USERS (username);


---------------- SCRATCH ----------------

select username, count(*)
from MONITOR_DW.MONDW_USERS
group by username
having count(*) > 1


select username, count(*)
from dba_users
group by username 
having count(*) > 1


select *
from dba_users
where username like 'TARGET%'


select *
from  MONITOR_DW.MONDW_USERS
where username = 'TARGET_POC'

delete from MONITOR_DW.MONDW_USERS
where user_id = 724

commit


