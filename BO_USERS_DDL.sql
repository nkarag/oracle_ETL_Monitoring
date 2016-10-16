create table monitor_dw.BO_USERS_20121003
(id number,
login varchar2(100),
fullname varchar(100),
email varchar2(100),
groups varchar2(4000),
disabled number(1),
never_connected number(1),
description varchar2(4000),
last_login date
);