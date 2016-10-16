drop index monitor_dw.OWB_ETLPA2_TMP2A_BIDX

alter session set statistics_level=all

with t3
as (
    select /*+ qb_name(seq_path_qry) inline parallel(t2 32)  */ CONNECT_BY_ISLEAF "IsLeaf2",
         LEVEL seq_length, SYS_CONNECT_BY_PATH(execution_name_short||' ['||trim(to_char(duration_mins))||', p80-'||trim(to_char(p80_duration_mins))||', TYPE:'||type||']', ' -->') SEQ_PATH,
         nkarag.EVAL(SYS_CONNECT_BY_PATH(NVL(TO_CHAR(duration_mins),'NULL'),'+')) PATH_DURATION_MINS,               
         t2.duration_mins + prior t2.duration_mins,
         T2.*
    from monitor_dw.owb_etlpa2_tmp2a t2 --t2
    where 1=1     
      -- exclude AND nodes as "previous nodes" in cases where 2 or more "previous nodes" exist
      AND not regexp_like( t2.prev_execution_name_short,  case when t2.cnt > 1 then '^\s*AND(\d|_)+.*' ELSE 'xoxoxo' end)
      --regexp_like (t2.prev_execution_name_short,  case when t2.cnt > 1 then '^AND\d+{0}?' ELSE '.*' end)
      -- exclude OR nodes as "previous nodes" in cases where 2 or more "previous nodes" exist
      AND not regexp_like( t2.prev_execution_name_short,  case when t2.cnt > 1 then '^\s*OR(\d|_)+.*' ELSE 'xoxoxo' end)                         
    start with t2.prev_created_on is null
    connect by NOCYCLE  t2.prev_execution_audit_id = prior t2.execution_audit_id             
),
t4
as (
    select /*+ qb_name(T4_block)  */  root_execution_name, TOP_LEVEL_EXECUTION_AUDIT_ID,
          root_created_on, 
          root_updated_on, 
        --  flow_level, 
          SEQ_PATH, seq_length, 
          max(seq_length) over(partition by root_created_on) maxlength,
          PATH_DURATION_MINS,
          max(PATH_DURATION_MINS) over(partition by root_created_on) max_path_dur_mins
    from t3
    order by root_created_on desc, seq_length desc               
)
select * from t4;


,
t5
as (
    select  
              row_number() over(partition by  root_created_on order by  path_duration_mins desc, seq_length desc) r,    
              root_execution_name, TOP_LEVEL_EXECUTION_AUDIT_ID,
              root_created_on, 
              root_updated_on, 
           --   flow_level, 
              PATH_DURATION_MINS,   
              seq_length,         
              SEQ_PATH CRITICAL_PATH     
    from t4
    where 1=1
      AND max_path_dur_mins = path_duration_mins
      --AND seq_length = maxlength -- relax this restriction because it is not true always and thus it prunes flow executions (missing days from the result)            
),
t6
as(
    select *
    from t5
    where
      r = 1    
    --order by root_created_on desc, path_duration_mins desc, seq_length desc
)
select /*+ qb_name(main)  */ *
from t4 --t6
order by root_execution_name, top_level_execution_audit_id desc;