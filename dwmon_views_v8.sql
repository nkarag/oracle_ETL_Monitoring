/**************************************

DWH usage monitoring views

2009-05-06
    1. add a "command" column to V_USER_SQL_EXECUTIONS_FCT to show the type of command
    2. add a command restriciton to V_NUM_OF_SQLS_PER_USER_PER_DAY to show only selects

2009-03-03
   1. add the same set of views for BO executions
   
2008-12-2008
 1. increase accuracy of V_USER_SQL_EXECUTIONS_FCT.ELAPSED_TIME_mins

2008-12-04
    1. corrected right outer join condition in all KPI views from: "tt.GEN_DIRECTORATE = tt.GEN_DIRECTORATE" to "tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE"
    2. Changed V_KPI_NUMOFLISTS_GD_MONTHLY to include a partitioned right outer join so that it shows 0 list activity for every month from go-live till sysdate  

2008-11-07
  1. add also "Read document" in the events tracked by 
  BO auditor

2008-09-03
 1. add views from BO auditor
2008-10-08
 1. add a view for owb objects
2008-10-10 (version 2)
 1. add more categories for users to KPI views
2008-10-15 (version 3)
 1. produce also daily report for all days in sysdate - 7 
  

last updated: 06/05/2009
author: nkarag

**************************************/

CREATE OR REPLACE VIEW V_USER_SQL_EXECUTIONS_old
(
YEAR
,MONTH
,DAY
,DAY_IN_MONTH
,SESS_SNAPSHOT_DATE
,SESS_SNAPSHOT_DATE_TIME
,USER_ID
,USER_ACCOUNT 
,USER_FULL_NAME
,OSUSER
,MACHINE 
,TERMINAL 
,PROGRAM 
,LOGON_TIME
,SQL_ID
,SQL_TEXT
,SQL_FULLTEXT 
,CPU_TIME 
,ELAPSED_TIME_mins 
)
AS
select to_char(sess.POPULATION_DATE, 'YYYY' ) as YEAR
       ,to_char(sess.POPULATION_DATE, 'MONTH' ) as MONTH
       ,to_char(sess.POPULATION_DATE, 'DAY' ) as DAY
       ,to_char(sess.POPULATION_DATE, 'DD' ) as DAY_IN_MONTH
       ,trunc(sess.POPULATION_DATE) as SESS_SNAPSHOT_DATE
       ,sess.POPULATION_DATE as SESS_SNAPSHOT_DATE_TIME
       ,sess.USER# as USER_ID
       ,sess.USERNAME as USER_ACCOUNT 
       ,usrs.FULL_NAME as USER_FULL_NAME
       ,sess.OSUSER as OSUSER
       ,sess.MACHINE as MACHINE 
        ,sess.TERMINAL as TERMINAL 
        ,sess.PROGRAM as PROGRAM 
        ,sess.LOGON_TIME as LOGON_TIME
        ,sess.SQL_ID as SQL_ID
        ,sqls.SQL_TEXT  as SQL_TEXT
        ,sqls.SQL_FULLTEXT as SQL_FULLTEXT 
        ,sqls.CPU_TIME as CPU_TIME
        ,to_char((sqls.ELAPSED_TIME/10000000)/60, '999') as ELAPSED_TIME_mins 
        --,(sqls.ELAPSED_TIME/1000000)/60 as ELAPSED_TIME_mins
from 
MONITOR_DW.MONDW_USERS usrs
JOIN
(
MONITOR_DW.MONDW_VSESSION_SNAP sess
    left outer join
    MONITOR_DW.MONDW_VSQLSTATS_SNAP sqls
    on (sess.SQL_ID = sqls.SQL_ID)
)   
ON (usrs.USER_ID = user#)


CREATE OR REPLACE VIEW V_USER_SQL_EXECUTIONS_FCT
(
YEAR
,MONTH
,DAY
,DAY_IN_MONTH
,SESS_SNAPSHOT_DATE
,SESS_SNAPSHOT_DATE_TIME
,USER_ID
,USER_ACCOUNT 
,USER_FULL_NAME
,OSUSER
,MACHINE 
,TERMINAL 
,PROGRAM 
,LOGON_TIME
,SQL_ID
,COMMAND
,SQL_TEXT
,SQL_FULLTEXT 
,CPU_TIME 
,ELAPSED_TIME_mins 
)
AS
select 
YEAR
,MONTH
,DAY
,DAY_IN_MONTH
,SESS_SNAPSHOT_DATE
,SESS_SNAPSHOT_DATE_TIME
,USER_ID
,USER_ACCOUNT 
,USER_FULL_NAME
,OSUSER
,MACHINE 
,TERMINAL 
,PROGRAM 
,LOGON_TIME
,SQL_ID
,COMMAND
,SQL_TEXT
,SQL_FULLTEXT 
,CPU_TIME 
,ELAPSED_TIME_mins 
from 
(
    select to_char(sess.POPULATION_DATE, 'YYYY' ) as YEAR
           ,to_char(sess.POPULATION_DATE, 'MONTH' ) as MONTH
           ,to_char(sess.POPULATION_DATE, 'DAY' ) as DAY
           ,to_char(sess.POPULATION_DATE, 'DD' ) as DAY_IN_MONTH
           ,trunc(sess.POPULATION_DATE) as SESS_SNAPSHOT_DATE
           ,sess.POPULATION_DATE as SESS_SNAPSHOT_DATE_TIME
           ,sess.USER# as USER_ID
           ,sess.USERNAME as USER_ACCOUNT 
           ,usrs.FULL_NAME as USER_FULL_NAME
           ,sess.OSUSER as OSUSER
           ,sess.MACHINE as MACHINE 
            ,sess.TERMINAL as TERMINAL 
            ,sess.PROGRAM as PROGRAM 
            ,sess.LOGON_TIME as LOGON_TIME
            ,sess.SQL_ID as SQL_ID
            ,sess.COMMAND as COMMAND
            ,sqls.SQL_TEXT  as SQL_TEXT
            ,sqls.SQL_FULLTEXT as SQL_FULLTEXT 
            ,sqls.CPU_TIME as CPU_TIME
            ,to_char((sqls.ELAPSED_TIME/10000000)/60, '9999') as ELAPSED_TIME_mins 
            --,(sqls.ELAPSED_TIME/1000000)/60 as ELAPSED_TIME_mins
            ,row_number() over(partition by sess.SID ,sess.SQL_ID ,sess.LOGON_TIME order by sess.POPULATION_DATE Desc ) as R
    from 
    MONITOR_DW.MONDW_USERS usrs
    JOIN
    (
    MONITOR_DW.MONDW_VSESSION_SNAP sess
        left outer join
        MONITOR_DW.MONDW_VSQLSTATS_SNAP sqls
        on (sess.SQL_ID = sqls.SQL_ID)
    )   
    ON (usrs.USER_ID = user#)
) tt
where
tt.R = 1


/**
Number of DWH users per day
*/
CREATE OR REPLACE VIEW V_NUM_OF_USERS_PER_DAY
(
YEAR
,MONTH
,DAY_IN_MONTH
,DAY 
,NUM_OF_USERS_PER_DAY
)
as
select fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH, fct.SESS_SNAPSHOT_DATE  ,count(distinct fct.USER_ID) 
from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT fct
group by fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH ,fct.SESS_SNAPSHOT_DATE


/**
Number of select statements executed per user per day
*/
CREATE OR REPLACE VIEW V_NUM_OF_SQLS_PER_USER_PER_DAY_old
(
YEAR
,MONTH
,DAY_IN_MONTH
,DAY
,USERNAME
,NUM_OF_SELECTS_EXECUTED
)
as
select fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH, fct.SESS_SNAPSHOT_DATE ,fct.USER_FULL_NAME, count(distinct fct.SQL_ID)
from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT fct
group by fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH, fct.SESS_SNAPSHOT_DATE ,fct.USER_FULL_NAME
order by fct.SESS_SNAPSHOT_DATE DESC

CREATE OR REPLACE VIEW V_NUM_OF_SQLS_PER_USER_PER_DAY
(
YEAR
,MONTH
,DAY_IN_MONTH
,DAY
,USERNAME
,USER_FULL_NAME
,NUM_OF_SELECTS_EXECUTED
)
as
select fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH, fct.SESS_SNAPSHOT_DATE ,fct.user_account,nvl(fct.USER_FULL_NAME,fct.user_account) ,count(fct.SQL_ID)
from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT fct
where command = 3 -- only selects
group by fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH, fct.SESS_SNAPSHOT_DATE ,fct.user_account ,nvl(fct.USER_FULL_NAME,fct.user_account)
order by fct.SESS_SNAPSHOT_DATE DESC


/**
Number of select statements executed per user per day - PIVOT1
*/

-- create pivot1 statement
select ',max(decode(USERNAME, '''||FULL_NAME||''', NUM_OF_SELECTS_EXECUTED,null)) as "'||FULL_NAME||'"'
from MONITOR_DW.MONDW_USERS

select
    fct.YEAR
    ,fct.MONTH
    ,fct.DAY_IN_MONTH
,max(decode(USERNAME, 'Matsas Giorgos ', NUM_OF_SELECTS_EXECUTED,null)) as "Matsas Giorgos "
,max(decode(USERNAME, 'Dimas Labros ', NUM_OF_SELECTS_EXECUTED,null)) as "Dimas Labros "
,max(decode(USERNAME, 'Chertouras Petros', NUM_OF_SELECTS_EXECUTED,null)) as "Chertouras Petros"
,max(decode(USERNAME, 'Troupis Panagiotis', NUM_OF_SELECTS_EXECUTED,null)) as "Troupis Panagiotis"
,max(decode(USERNAME, 'Bolomiti Evie ', NUM_OF_SELECTS_EXECUTED,null)) as "Bolomiti Evie "
,max(decode(USERNAME, 'Giannopoulou Aggeliki', NUM_OF_SELECTS_EXECUTED,null)) as "Giannopoulou Aggeliki"
,max(decode(USERNAME, 'Moustakas Kostas', NUM_OF_SELECTS_EXECUTED,null)) as "Moustakas Kostas"
,max(decode(USERNAME, 'Livaniou Athina', NUM_OF_SELECTS_EXECUTED,null)) as "Livaniou Athina"
,max(decode(USERNAME, 'Batsila Anna', NUM_OF_SELECTS_EXECUTED,null)) as "Batsila Anna"
,max(decode(USERNAME, 'Derembei Ermina', NUM_OF_SELECTS_EXECUTED,null)) as "Derembei Ermina"
,max(decode(USERNAME, 'Paraschos Aggelos', NUM_OF_SELECTS_EXECUTED,null)) as "Paraschos Aggelos"
,max(decode(USERNAME, 'Mavridis Konstantinos', NUM_OF_SELECTS_EXECUTED,null)) as "Mavridis Konstantinos"
,max(decode(USERNAME, '×. ÂÁÓÉËÅÉÁÄÏÕ', NUM_OF_SELECTS_EXECUTED,null)) as "×. ÂÁÓÉËÅÉÁÄÏÕ"
,max(decode(USERNAME, 'Ä. ×ÑÇÓÔÉÄÇÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Ä. ×ÑÇÓÔÉÄÇÓ"
,max(decode(USERNAME, 'Å. ÅËÉÅÆÅÑ', NUM_OF_SELECTS_EXECUTED,null)) as "Å. ÅËÉÅÆÅÑ"
,max(decode(USERNAME, 'Ì. ÃÁÓÐÁÑÇ', NUM_OF_SELECTS_EXECUTED,null)) as "Ì. ÃÁÓÐÁÑÇ"
,max(decode(USERNAME, 'Å. ÌÁÍÙËÁÊÁÊÇ', NUM_OF_SELECTS_EXECUTED,null)) as "Å. ÌÁÍÙËÁÊÁÊÇ"
,max(decode(USERNAME, 'Ó. ×ÁÔÆÇÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Ó. ×ÁÔÆÇÓ"
,max(decode(USERNAME, 'Å. ÐÁÐÐÏÕÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Å. ÐÁÐÐÏÕÓ"
,max(decode(USERNAME, 'Å. ÊÏÑÏÌÇËÁ', NUM_OF_SELECTS_EXECUTED,null)) as "Å. ÊÏÑÏÌÇËÁ"
,max(decode(USERNAME, 'Ì. ØÕËÁÊÇ', NUM_OF_SELECTS_EXECUTED,null)) as "Ì. ØÕËÁÊÇ"
,max(decode(USERNAME, 'Á. ÌÁÕÑÁÍÔÙÍÁÊÇ', NUM_OF_SELECTS_EXECUTED,null)) as "Á. ÌÁÕÑÁÍÔÙÍÁÊÇ"
,max(decode(USERNAME, 'Ä. ÆÁÑÌÐÇÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Ä. ÆÁÑÌÐÇÓ"
,max(decode(USERNAME, 'Ã. ÂÁÓÉËÁÑÁ', NUM_OF_SELECTS_EXECUTED,null)) as "Ã. ÂÁÓÉËÁÑÁ"
,max(decode(USERNAME, 'Ð. ÌÁÑÊÏÐÏÕËÏÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Ð. ÌÁÑÊÏÐÏÕËÏÓ"
,max(decode(USERNAME, 'Ó. ÌÐÏÈÏÕ', NUM_OF_SELECTS_EXECUTED,null)) as "Ó. ÌÐÏÈÏÕ"
,max(decode(USERNAME, 'Á. ÂÅÑÏÐÏÕËÏÕ', NUM_OF_SELECTS_EXECUTED,null)) as "Á. ÂÅÑÏÐÏÕËÏÕ"
,max(decode(USERNAME, 'Á. ÖÏÕÑËÁÑÇÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Á. ÖÏÕÑËÁÑÇÓ"
,max(decode(USERNAME, 'Ê. ÌÁÕÑÏÃÉÁÍÍÁÊÇ', NUM_OF_SELECTS_EXECUTED,null)) as "Ê. ÌÁÕÑÏÃÉÁÍÍÁÊÇ"
,max(decode(USERNAME, 'Ã. ÌÏÕÑÉÊÇÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Ã. ÌÏÕÑÉÊÇÓ"
,max(decode(USERNAME, 'Â. ÓÊËÇÊÁ', NUM_OF_SELECTS_EXECUTED,null)) as "Â. ÓÊËÇÊÁ"
,max(decode(USERNAME, 'Ð. ÊÁÑÁÌÐÏÕÍÁÑËÇÓ', NUM_OF_SELECTS_EXECUTED,null)) as "Ð. ÊÁÑÁÌÐÏÕÍÁÑËÇÓ"
,max(decode(USERNAME, 'Karagiannidis Nikos', NUM_OF_SELECTS_EXECUTED,null)) as "Karagiannidis Nikos"
,max(decode(USERNAME, 'Sinos Loukas', NUM_OF_SELECTS_EXECUTED,null)) as "Sinos Loukas"
,max(decode(USERNAME, 'Zorbas Vaggelis', NUM_OF_SELECTS_EXECUTED,null)) as "Zorbas Vaggelis"
,max(decode(USERNAME, 'Fotopoulos Eleftherios', NUM_OF_SELECTS_EXECUTED,null)) as "Fotopoulos Eleftherios"
from
V_NUM_OF_SQLS_PER_USER_PER_DAY fct
group by 
    fct.YEAR
    ,fct.MONTH
    ,fct.DAY_IN_MONTH


/**
Number of select statements executed per user per day - PIVOT2
*/

-- create pivot2 statement
select ',max(decode(DAY, '''||dt||''', NUM_OF_SELECTS_EXECUTED,null)) as "'||dt||'"'
from (select distinct trunc(SESS_SNAPSHOT_DATE) as dt from MONITOR_DW.V_USER_SQL_EXECUTIONS) tt 
order by dt ASC

select ',"'||dt||'"'
from (select distinct trunc(SESS_SNAPSHOT_DATE) as dt from MONITOR_DW.V_USER_SQL_EXECUTIONS) tt 
order by dt ASC


CREATE OR REPLACE VIEW V_NUMSQLS_PER_USER_PER_DAY_PVT
(
USERNAME
,"23-JUN-08"
,"24-JUN-08"
,"25-JUN-08"
,"26-JUN-08"
,"27-JUN-08"
,"01-JUL-08"
,"02-JUL-08"
)
as
select
    u.FULL_NAME
,max(decode(DAY, '23-JUN-08', NUM_OF_SELECTS_EXECUTED,null)) as "23-JUN-08"
,max(decode(DAY, '24-JUN-08', NUM_OF_SELECTS_EXECUTED,null)) as "24-JUN-08"
,max(decode(DAY, '25-JUN-08', NUM_OF_SELECTS_EXECUTED,null)) as "25-JUN-08"
,max(decode(DAY, '26-JUN-08', NUM_OF_SELECTS_EXECUTED,null)) as "26-JUN-08"
,max(decode(DAY, '27-JUN-08', NUM_OF_SELECTS_EXECUTED,null)) as "27-JUN-08"
,max(decode(DAY, '01-JUL-08', NUM_OF_SELECTS_EXECUTED,null)) as "01-JUL-08"
,max(decode(DAY, '02-JUL-08', NUM_OF_SELECTS_EXECUTED,null)) as "02-JUL-08"
from
V_NUM_OF_SQLS_PER_USER_PER_DAY fct right outer join MONITOR_DW.MONDW_USERS u on(fct.USERNAME = u.FULL_NAME)
group by 
    u.FULL_NAME
    
delete from  MONITOR_DW.MONDW_MONDW_USERS USERS
where   MONITOR_DW.MONDW_USERS.FULL_NAME is null

delete from  MONITOR_DW.MONDW_USERS
where   user_id = 137

commit
    

/*
From BO auditor

One row per report opened, per user and the duration

*/
CREATE OR REPLACE VIEW V_USER_BO_REPORT_EXECTNS_FCT
(
event_id
,duration_in_mins
,report_name
,username
,username_bo
,event_description
,event_start_timestamp
)
AS 
SELECT 
  AUDIT_EVENT.Event_ID, 
  to_char(AUDIT_EVENT.Duration/60, '999'), 
  DERIVED_DOCUMENT_NAME.Detail_Text, 
  upper(AUDIT_EVENT.User_Name),
  AUDIT_EVENT.User_Name, 
  EVENT_TYPE.Event_Type_Description, 
  AUDIT_EVENT.Start_Timestamp 
FROM 
  boxiaudit.AUDIT_EVENT, 
  ( 
  select 
                 AUDIT_EVENT.Server_CUID, AUDIT_EVENT.Event_ID, 
RTRIM(TO_CHAR(AUDIT_DETAIL.Detail_Text), CHR(0)) as Detail_Text 
from 
                 boxiaudit.AUDIT_EVENT, boxiaudit.AUDIT_DETAIL 
where 
                 (AUDIT_EVENT.Server_CUID = AUDIT_DETAIL.Server_CUID) and 
                 (AUDIT_EVENT.Event_ID = AUDIT_DETAIL.Event_ID) and 
                 (AUDIT_DETAIL.Detail_Type_ID = 8) 
  )  DERIVED_DOCUMENT_NAME, 
  boxiaudit.EVENT_TYPE 
WHERE 
  ( AUDIT_EVENT.Event_Type_ID=EVENT_TYPE.Event_Type_ID  ) 
  AND  ( AUDIT_EVENT.Event_ID=DERIVED_DOCUMENT_NAME.Event_ID and 
AUDIT_EVENT.Server_CUID=DERIVED_DOCUMENT_NAME.Server_CUID  ) 
  AND 
  AUDIT_EVENT.Event_Type_ID  in  (19,11) -- (Document refresh, Read Document)
--  AND DETAIL_TEXT='XXX' 
order by AUDIT_EVENT.Start_Timestamp desc
/

/**
Number of DWH users per day (from BO executions)
*/
CREATE OR REPLACE VIEW V_NUMOFUSERSPERDAY_BO_EXECTNS
(
YEAR
,MONTH
,DAY_IN_MONTH
,DAY 
,NUM_OF_USERS_PER_DAY
)
as
select extract(year from fct.EVENT_START_TIMESTAMP) as year 
    , extract(month from fct.EVENT_START_TIMESTAMP) as month
    , extract(day from fct.EVENT_START_TIMESTAMP) as day_in_month
    , trunc(fct.EVENT_START_TIMESTAMP) as snapshot_date  
    ,count(distinct fct.username) 
from MONITOR_DW.V_USER_BO_REPORT_EXECTNS_FCT fct
group by extract(year from fct.EVENT_START_TIMESTAMP)  
    , extract(month from fct.EVENT_START_TIMESTAMP) 
    , extract(day from fct.EVENT_START_TIMESTAMP) 
    , trunc(fct.EVENT_START_TIMESTAMP)   


/**
Number of select statements executed per user per day (from BO executions)
*/
CREATE OR REPLACE VIEW V_NUMOF_BO_EXECTNS_PUSERPDAY
(
YEAR
,MONTH
,DAY_IN_MONTH
,DAY
,USERNAME
,NUM_OF_BO_EXECTNS
)
as
select extract(year from fct.EVENT_START_TIMESTAMP) as year 
    , extract(month from fct.EVENT_START_TIMESTAMP) as month
    , extract(day from fct.EVENT_START_TIMESTAMP) as day_in_month
    , trunc(fct.EVENT_START_TIMESTAMP) as snapshot_date
    ,fct.username as username
     ,count(fct.event_ID) -- number of Document Refreshes or document reads
from MONITOR_DW.V_USER_BO_REPORT_EXECTNS_FCT fct
group by extract(year from fct.EVENT_START_TIMESTAMP)  
    , extract(month from fct.EVENT_START_TIMESTAMP) 
    , extract(day from fct.EVENT_START_TIMESTAMP) 
    , trunc(fct.EVENT_START_TIMESTAMP) 
    ,fct.username 
order by SNAPSHOT_DATE DESC


/***
 ***********************   KPIs SECTION *********************************
***/

/*
Number of SQLs per GD per month
*/
create or replace view V_KPI_NUM_SQL_PER_GD_MONTHLY
(
y
,mon
,mon_num
,FUNCT_GROUP
,LOGICAL_GROUP
,GEN_DIRECTORATE
,DIRECTORATE
,num_of_selects
)
as
select 
        tt.year
        ,tt.month
        ,tt.mon_num
        ,uu.FUNCT_GROUP
        ,uu.LOGICAL_GROUP
        ,uu.GEN_DIRECTORATE
        ,uu.DIRECTORATE
        ,nvl(num_of_selects, 0)
from 
(select year, month, to_char(day, 'mm' ) mon_num, FUNCT_GROUP,LOGICAL_GROUP,GEN_DIRECTORATE,DIRECTORATE  
    ,sum(num_of_selects_executed) num_of_selects 
from MONITOR_DW.V_NUM_OF_SQLS_PER_USER_PER_DAY join MONITOR_DW.MONDW_USERS using(username)
group by year, month, to_char(day, 'mm' )
        , FUNCT_GROUP,LOGICAL_GROUP,GEN_DIRECTORATE,DIRECTORATE
) tt 
partition by (tt.year, tt.month, tt.mon_num) 
right outer join 
(select distinct GEN_DIRECTORATE, DIRECTORATE ,FUNCT_GROUP ,LOGICAL_GROUP 
from MONITOR_DW.MONDW_USERS
where GEN_DIRECTORATE is not null and DIRECTORATE is not null and FUNCT_GROUP is not null and LOGICAL_GROUP is not null  
) uu 
on(tt.FUNCT_GROUP = uu.FUNCT_GROUP and tt.LOGICAL_GROUP = uu.LOGICAL_GROUP and
                                               tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 6, 7
--order by to_date(year||'-'||month, 'YYYY-Month')

/*
Number of BO Executions (document refreshes, document reads) per GD per month
*/
create or replace view V_KPI_NUM_BOEX_PER_GD_MONTHLY
(
y
,mon
,mon_num
,FUNCT_GROUP
,LOGICAL_GROUP
,GEN_DIRECTORATE
,DIRECTORATE
,num_of_bo_exs
)
as
select 
        tt.year
        ,tt.month
        ,tt.mon_num
        ,uu.FUNCT_GROUP
        ,uu.LOGICAL_GROUP
        ,uu.GEN_DIRECTORATE
        ,uu.DIRECTORATE
        ,nvl(num_of_bo_exs, 0)
from 
(select year, month, to_char(day, 'mm' ) mon_num, FUNCT_GROUP,LOGICAL_GROUP,GEN_DIRECTORATE,DIRECTORATE  
    ,sum(NUM_OF_BO_EXECTNS) num_of_bo_exs 
from MONITOR_DW.V_NUMOF_BO_EXECTNS_PUSERPDAY join MONITOR_DW.MONDW_USERS using(username)
group by year, month, to_char(day, 'mm' )
        , FUNCT_GROUP,LOGICAL_GROUP,GEN_DIRECTORATE,DIRECTORATE
) tt 
partition by (tt.year, tt.month, tt.mon_num) 
right outer join 
(select distinct GEN_DIRECTORATE, DIRECTORATE ,FUNCT_GROUP ,LOGICAL_GROUP 
from MONITOR_DW.MONDW_USERS
where GEN_DIRECTORATE is not null and DIRECTORATE is not null and FUNCT_GROUP is not null and LOGICAL_GROUP is not null  
) uu 
on(tt.FUNCT_GROUP = uu.FUNCT_GROUP and tt.LOGICAL_GROUP = uu.LOGICAL_GROUP and
                                               tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 6, 7


/*
Number of SQLs per GD per day 
*/
create or replace view V_KPI_NUM_SQL_PER_GD_DAILY
(
y
,mon
,mon_num
,day
,FUNCT_GROUP
,LOGICAL_GROUP
,GEN_DIRECTORATE
,DIRECTORATE
,num_of_selects
)
as
select 
        tt.year
        ,tt.month
        ,tt.mon_num
        ,tt.day
        ,uu.FUNCT_GROUP
        ,uu.LOGICAL_GROUP
        ,uu.GEN_DIRECTORATE
        ,uu.DIRECTORATE
        ,nvl(num_of_selects, 0)
from 
(select year, month, to_char(day, 'mm' ) mon_num, trunc(day) day,  FUNCT_GROUP,LOGICAL_GROUP,GEN_DIRECTORATE,DIRECTORATE  
    ,sum(num_of_selects_executed) num_of_selects 
from MONITOR_DW.V_NUM_OF_SQLS_PER_USER_PER_DAY join MONITOR_DW.MONDW_USERS using(username)
where trunc(day) >= trunc(sysdate) - 7
group by year, month, to_char(day, 'mm' ), trunc(day)
        , FUNCT_GROUP,LOGICAL_GROUP,GEN_DIRECTORATE,DIRECTORATE
) tt 
partition by (tt.year, tt.month, tt.mon_num, tt.day) 
right outer join 
(select distinct GEN_DIRECTORATE, DIRECTORATE ,FUNCT_GROUP ,LOGICAL_GROUP 
from MONITOR_DW.MONDW_USERS
where GEN_DIRECTORATE is not null and DIRECTORATE is not null and FUNCT_GROUP is not null and LOGICAL_GROUP is not null  
) uu 
on(tt.FUNCT_GROUP = uu.FUNCT_GROUP and tt.LOGICAL_GROUP = uu.LOGICAL_GROUP and
                                               tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 4, 7, 8


/*
Number of distinct users connected per GD per month
*/
create or replace view V_KPI_NUMUSERS_PER_GD_MONTHLY
(
year
,month
,mon_num
,FUNCT_GROUP
,LOGICAL_GROUP
,GEN_DIRECTORATE
,DIRECTORATE
,num_of_users
) 
as
select
    tt.year
    ,tt.month
    ,tt.mon_num
    ,uu.FUNCT_GROUP
    ,uu.LOGICAL_GROUP
    ,uu.GEN_DIRECTORATE
    ,uu.DIRECTORATE
    ,nvl(tt.num_of_users,0)
from
(
    select year , month,  to_char(SESS_SNAPSHOT_DATE, 'mm' ) mon_num, FUNCT_GROUP,LOGICAL_GROUP, GEN_DIRECTORATE ,DIRECTORATE  
        ,count(distinct fct.USER_ID) num_of_users 
    from  MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT fct join MONITOR_DW.MONDW_USERS usr on(fct.user_account = usr.username)    
    group by year, month,  to_char(SESS_SNAPSHOT_DATE, 'mm' ), FUNCT_GROUP,LOGICAL_GROUP, GEN_DIRECTORATE ,DIRECTORATE
) tt
partition by (tt.year, tt.month, tt.mon_num) 
right outer join 
(
    select distinct GEN_DIRECTORATE, DIRECTORATE ,FUNCT_GROUP ,LOGICAL_GROUP 
    from MONITOR_DW.MONDW_USERS
    where GEN_DIRECTORATE is not null and DIRECTORATE is not null and FUNCT_GROUP is not null and LOGICAL_GROUP is not null  
) uu 
on(tt.FUNCT_GROUP = uu.FUNCT_GROUP and tt.LOGICAL_GROUP = uu.LOGICAL_GROUP and
                                               tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 6, 7

/*
Number of distinct users connected per GD per month (from BO executions)
*/
create or replace view V_KPI_NUMUSERSP_GD_MON_BOEXS
(
year
,month
,mon_num
,FUNCT_GROUP
,LOGICAL_GROUP
,GEN_DIRECTORATE
,DIRECTORATE
,num_of_users
) 
as
select
    tt.year
    ,tt.month
    ,tt.mon_num
    ,uu.FUNCT_GROUP
    ,uu.LOGICAL_GROUP
    ,uu.GEN_DIRECTORATE
    ,uu.DIRECTORATE
    ,nvl(tt.num_of_users,0)
from
(
    select   extract(year from fct.EVENT_START_TIMESTAMP) as year
            ,to_char( fct.EVENT_START_TIMESTAMP, 'MONTH') as month
            , extract(month from fct.EVENT_START_TIMESTAMP) as mon_num
            , FUNCT_GROUP,LOGICAL_GROUP
            , GEN_DIRECTORATE 
            ,DIRECTORATE  
            ,count(distinct fct.username) num_of_users         
    from  MONITOR_DW.V_USER_BO_REPORT_EXECTNS_FCT fct join MONITOR_DW.MONDW_USERS usr on(fct.username = usr.username)    
    group by extract(year from fct.EVENT_START_TIMESTAMP)
            ,to_char( fct.EVENT_START_TIMESTAMP, 'MONTH')
            , extract(month from fct.EVENT_START_TIMESTAMP)
            , FUNCT_GROUP,LOGICAL_GROUP
            , GEN_DIRECTORATE 
            ,DIRECTORATE
) tt
partition by (tt.year, tt.month, tt.mon_num) 
right outer join 
(
    select distinct GEN_DIRECTORATE, DIRECTORATE ,FUNCT_GROUP ,LOGICAL_GROUP 
    from MONITOR_DW.MONDW_USERS
    where GEN_DIRECTORATE is not null and DIRECTORATE is not null and FUNCT_GROUP is not null and LOGICAL_GROUP is not null  
) uu 
on(tt.FUNCT_GROUP = uu.FUNCT_GROUP and tt.LOGICAL_GROUP = uu.LOGICAL_GROUP and
                                               tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 6, 7

--select extract(month from sysdate), to_char(sysdate, 'MONTH') 
--from dual

/*
Number of distinct users connected per GD per day
*/
create or replace view V_KPI_NUMUSERS_PER_GD_DAILY
(
year
,month
,mon_num
,day
,FUNCT_GROUP
,LOGICAL_GROUP
,GEN_DIRECTORATE
,DIRECTORATE
,num_of_users
) 
as
select
    tt.year
    ,tt.month
    ,tt.mon_num
    ,tt.day
    ,uu.FUNCT_GROUP
    ,uu.LOGICAL_GROUP
    ,uu.GEN_DIRECTORATE
    ,uu.DIRECTORATE
    ,nvl(tt.num_of_users,0)
from
(
    select year , month,  to_char(SESS_SNAPSHOT_DATE, 'mm' ) mon_num, trunc(SESS_SNAPSHOT_DATE) day, FUNCT_GROUP,LOGICAL_GROUP, GEN_DIRECTORATE ,DIRECTORATE  
        ,count(distinct fct.USER_ID) num_of_users 
    from  MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT fct join MONITOR_DW.MONDW_USERS usr on(fct.user_account = usr.username)
    where trunc(SESS_SNAPSHOT_DATE) >= trunc(sysdate) - 7
    group by year, month,  to_char(SESS_SNAPSHOT_DATE, 'mm' ), trunc(SESS_SNAPSHOT_DATE), FUNCT_GROUP,LOGICAL_GROUP, GEN_DIRECTORATE ,DIRECTORATE
) tt
partition by (tt.year, tt.month, tt.mon_num, tt.day) 
right outer join 
(
    select distinct GEN_DIRECTORATE, DIRECTORATE ,FUNCT_GROUP ,LOGICAL_GROUP 
    from MONITOR_DW.MONDW_USERS
    where GEN_DIRECTORATE is not null and DIRECTORATE is not null and FUNCT_GROUP is not null and LOGICAL_GROUP is not null  
) uu 
on(tt.FUNCT_GROUP = uu.FUNCT_GROUP and tt.LOGICAL_GROUP = uu.LOGICAL_GROUP and
                                               tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 4, 7, 8

/*
Number of distinct users connected per GD per day (from BO Executions)
*/
create or replace view V_KPI_NUMUSERSP_GD_DAILY_BOEXS
(
year
,month
,mon_num
,day
,FUNCT_GROUP
,LOGICAL_GROUP
,GEN_DIRECTORATE
,DIRECTORATE
,num_of_users
) 
as
select
    tt.year
    ,tt.month
    ,tt.mon_num
    ,tt.day
    ,uu.FUNCT_GROUP
    ,uu.LOGICAL_GROUP
    ,uu.GEN_DIRECTORATE
    ,uu.DIRECTORATE
    ,nvl(tt.num_of_users,0)
from
(
    select extract(year from fct.EVENT_START_TIMESTAMP) as year
            ,to_char( fct.EVENT_START_TIMESTAMP, 'MONTH') as month
            , extract(month from fct.EVENT_START_TIMESTAMP) as mon_num
            , trunc(fct.EVENT_START_TIMESTAMP) as day
            , FUNCT_GROUP
            ,LOGICAL_GROUP
            , GEN_DIRECTORATE 
            ,DIRECTORATE  
            ,count(distinct fct.username) num_of_users 
    from  MONITOR_DW.V_USER_BO_REPORT_EXECTNS_FCT fct join MONITOR_DW.MONDW_USERS usr on(fct.username = usr.username)
    where trunc(EVENT_START_TIMESTAMP) >= trunc(sysdate) - 7
    group by 
    extract(year from fct.EVENT_START_TIMESTAMP)
            ,to_char( fct.EVENT_START_TIMESTAMP, 'MONTH')
            , extract(month from fct.EVENT_START_TIMESTAMP)
            , trunc(fct.EVENT_START_TIMESTAMP)
            , FUNCT_GROUP
            ,LOGICAL_GROUP
            , GEN_DIRECTORATE 
            ,DIRECTORATE              
) tt
partition by (tt.year, tt.month, tt.mon_num, tt.day) 
right outer join 
(
    select distinct GEN_DIRECTORATE, DIRECTORATE ,FUNCT_GROUP ,LOGICAL_GROUP 
    from MONITOR_DW.MONDW_USERS
    where GEN_DIRECTORATE is not null and DIRECTORATE is not null and FUNCT_GROUP is not null and LOGICAL_GROUP is not null  
) uu 
on(tt.FUNCT_GROUP = uu.FUNCT_GROUP and tt.LOGICAL_GROUP = uu.LOGICAL_GROUP and
                                               tt.GEN_DIRECTORATE = uu.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 4, 7, 8


/*
Lists Created from the DWH after go-live 
*/
create or replace view V_TLM_LISTS_CREATED_AFTER_LIVE
(TM_PROCESS_ID,  TM_PROCESS_DATE ,USERNAME ,TM_LISTNAME)
as
select TM_PROCESS_ID,  TM_PROCESS_DATE ,upper(TM_USERNAME) ,TM_LISTNAME 
from TELEM_DW.TM_INTERMEDIATE_PROCESS_DIM
where trunc(TM_PROCESS_DATE) > to_date('23/06/2008', 'dd/mm/yyyy')
order by 2 desc

/*
Number of lists created from the DWH per month and GD
*/
create or replace view V_KPI_NUMOFLISTS_GD_MONTHLY
(
year
,month
,mon_num
,Gen_Directorate
,Directorate
,num_of_lists
)as
select 
    uu.year_num
    ,uu.MONTH_NAME_GR
    ,uu.month_num
    ,tt.GEN_DIRECTORATE
    ,tt.DIRECTORATE
    ,nvl(tt.num_of_list,0)
from
(
select 
    to_char(TM_PROCESS_DATE, 'YYYY') year
    ,to_char(TM_PROCESS_DATE, 'Month') month
    ,to_char(TM_PROCESS_DATE, 'mm') mon_num
    ,DIRECTORATE
    ,GEN_DIRECTORATE 
    ,count(distinct  TM_PROCESS_ID) num_of_list 
from MONITOR_DW.V_TLM_LISTS_CREATED_AFTER_LIVE join MONITOR_DW.MONDW_USERS using(username)
group by
    to_char(TM_PROCESS_DATE, 'YYYY')
    ,to_char(TM_PROCESS_DATE, 'Month')
    ,to_char(TM_PROCESS_DATE, 'mm')
    ,DIRECTORATE
    ,GEN_DIRECTORATE 
) tt
partition by (tt.GEN_DIRECTORATE, tt.DIRECTORATE) 
right outer join 
(
    select distinct YEAR_NUM, MONTH_NUM ,MONTH_NAME_GR 
    from target_dw.date_dim
    where   DATE_KEY between to_date('23/06/2008', 'dd/mm/yyyy') and trunc(sysdate)
) uu 
on(tt.year = uu.YEAR_NUM and tt.mon_num = uu.MONTH_NUM)    
order by 1, 3, 4, 5


/* previous (before version 5) definition
create or replace view V_KPI_NUMOFLISTS_GD_MONTHLY
(
year
,month
,mon_num
,Gen_Directorate
,num_of_lists
)as
select 
    to_char(TM_PROCESS_DATE, 'YYYY')
    ,to_char(TM_PROCESS_DATE, 'Month')
    ,to_char(TM_PROCESS_DATE, 'mm')
    ,user_category
    ,count(distinct  TM_PROCESS_ID) num_of_list 
from MONITOR_DW.V_TLM_LISTS_CREATED_AFTER_LIVE join MONITOR_DW.MONDW_USERS using(username)
group by
    to_char(TM_PROCESS_DATE, 'YYYY')
    ,to_char(TM_PROCESS_DATE, 'Month')
    ,to_char(TM_PROCESS_DATE, 'mm')
    ,user_category 
*/
/*
select 
    tt.year
    ,tt.month
    ,tt.mon_num
    ,uu.GEN_DIRECTORATE
    ,uu.DIRECTORATE
    ,nvl(tt.num_of_list,0)
from
(
select 
    to_char(TM_PROCESS_DATE, 'YYYY') year
    ,to_char(TM_PROCESS_DATE, 'Month') month
    ,to_char(TM_PROCESS_DATE, 'mm') mon_num
    ,DIRECTORATE
    ,GEN_DIRECTORATE 
    ,count(distinct  TM_PROCESS_ID) num_of_list 
from MONITOR_DW.V_TLM_LISTS_CREATED_AFTER_LIVE join MONITOR_DW.MONDW_USERS using(username)
group by
    to_char(TM_PROCESS_DATE, 'YYYY')
    ,to_char(TM_PROCESS_DATE, 'Month')
    ,to_char(TM_PROCESS_DATE, 'mm')
    ,DIRECTORATE
    ,GEN_DIRECTORATE 
) tt
partition by (tt.year, tt.month, tt.mon_num) 
right outer join 
(
    select distinct GEN_DIRECTORATE, DIRECTORATE 
    from MONITOR_DW.MONDW_USERS
    where GEN_DIRECTORATE is not null and DIRECTORATE is not null and GEN_DIRECTORATE in ('ÃÄÏÐ')  
) uu 
on(tt.GEN_DIRECTORATE = tt.GEN_DIRECTORATE and tt.DIRECTORATE = uu.DIRECTORATE)    
order by 1, 3, 4, 5
*/




/*
Customer Segmentation Process executed from the DWH after go-live 
*/
create or replace view V_SEGM_PROC_EXEC_AFTER_LIVE
(SEGMENTATION_PROCEDURE_SK,  SEGMENTATION_PROCEDURE_DESC,  SEGMENTATION_DATE)
as
select SEGMENTATION_PROCEDURE_SK,  SEGMENTATION_PROCEDURE_DESC,  SEGMENTATION_DATE 
from TARGET_DW.SEGMENTATION_PROCEDURE_DIM 
where trunc(SEGMENTATION_DATE) > to_date('23/06/2008', 'dd/mm/yyyy')  

/*
Num of Customer Segmentation Process executed from the DWH after go-live per month 
*/

create or replace view V_KPI_NUMOF_SEGM_PROC_MONTHLY
(
year
,month
,mon_num
,num_of_segm_proc
)as
select 
    to_char(SEGMENTATION_DATE, 'YYYY')
    ,to_char(SEGMENTATION_DATE, 'Month')
    ,to_char(SEGMENTATION_DATE, 'mm')
    ,count(distinct  SEGMENTATION_PROCEDURE_SK ) num_of_segm_proc 
from V_SEGM_PROC_EXEC_AFTER_LIVE
group by
    to_char(SEGMENTATION_DATE, 'YYYY')
    ,to_char(SEGMENTATION_DATE, 'Month')
    ,to_char(SEGMENTATION_DATE, 'mm')

/*
View for OWB objects. Use object_name predicate otherwise it is real slow!
*/
create or replace view V_OWB_ALL_IV_ALL_OBJECTS
(OBJECT_ID, OBJECT_UOID, OBJECT_TYPE, SCRIPTING_TYPE, OBJECT_NAME, 
 BUSINESS_NAME, DESCRIPTION, PARENT_OBJECT_ID, PARENT_OBJECT_TYPE, PARENT_SCRIPTING_TYPE, 
 PARENT_OBJECT_NAME, IS_VALID, UPDATED_ON, CREATED_ON, UPDATED_BY, 
 CREATED_BY)
as 
select * from 
owb.ALL_IV_ALL_OBJECTS

----------------------------------- SCRATCH
select year , month,  to_char(SESS_SNAPSHOT_DATE, 'mm' ), user_category, count(distinct fct.USER_ID) 
from  MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT fct join MONITOR_DW.MONDW_USERS usr on(fct.user_account = usr.username)
group by year, month,  to_char(SESS_SNAPSHOT_DATE, 'mm' ), user_category



select fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH, fct.SESS_SNAPSHOT_DATE  ,count(distinct fct.USER_ID) 
from MONITOR_DW.V_USER_SQL_EXECUTIONS_FCT fct
group by fct.YEAR, fct.MONTH, fct.DAY_IN_MONTH ,fct.SESS_SNAPSHOT_DATE
