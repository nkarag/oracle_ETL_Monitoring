-- 1. Partitioning

select partitioning_type, subpartitioning_type, count(*)
from dba_part_tables
where owner = '&data_mart'
group by partitioning_type, subpartitioning_type

-- 2    DOP of segments (tables/indexes)

select degree, count(*)
from dba_tables
where owner = '&data_mart'
group by degree


select degree, table_name
from dba_tables
where owner = '&data_mart'
and partitioned = 'YES'


-- 3    Physical attributes των segments

select table_name, pct_free, INI_TRANS
from dba_tables
where owner = '&data_mart'

select index_name, pct_free, INI_TRANS
from dba_indexes
where owner = '&data_mart'

select table_name, pct_free, INI_TRANS
from dba_tab_partitions
where table_owner = '&data_mart'

select index_name, pct_free, INI_TRANS
from dba_ind_partitions
where index_owner = '&data_mart'

-- 4    Compression / ILM

select segment_name, sum(bytes)/1024/1024/1024 
from dba_segments 
where owner = '&data_mart' and segment_type like 'TABLE%'
group by segment_name
order by 2 desc


select table_name, COMPRESSION, COMPRESS_FOR
from dba_tables
where owner = '&data_mart'

select table_name, COMPRESSION, COMPRESS_FOR
from dba_tab_partitions
where table_owner = '&data_mart'

select table_name, COMPRESSION, COMPRESS_FOR
from dba_tables t1
where owner = '&data_mart' and table_name like '%DIM'
and (select sum(bytes)/1024/1024/1024 from dba_segments where owner = '&data_mart' and segment_name = t1.table_name and segment_type like 'TABLE%') > 1  
union
select table_name, COMPRESSION, COMPRESS_FOR
from dba_tab_partitions p1
where table_owner = '&data_mart' and table_name like '%DIM'
and (select sum(bytes)/1024/1024/1024 from dba_segments where owner = '&data_mart' and segment_name = p1.table_name and segment_type like 'TABLE%') > 1 
order by 1


select table_name, COMPRESSION, COMPRESS_FOR
from dba_tables t1
where owner = '&data_mart' and table_name like '%FCT'
and (select sum(bytes)/1024/1024/1024 from dba_segments where owner = '&data_mart' and segment_name = t1.table_name and segment_type like 'TABLE%') > 1 
union
select table_name, COMPRESSION, COMPRESS_FOR
from dba_tab_partitions p1
where table_owner = '&data_mart' and table_name like '%FCT'
and (select sum(bytes)/1024/1024/1024 from dba_segments where owner = '&data_mart' and segment_name = p1.table_name and segment_type like 'TABLE%') > 1 
order by 1


--  5    Constraints
select constraint_type, status, validated, rely, count(*) 
from dba_constraints
where OWNER = '&data_mart'
group by constraint_type, status, validated, rely

-- mising PKs
select table_name
from dba_tables
where 
table_name not in (
    select table_name
    from dba_constraints
    where
        OWNER = '&data_mart'
        and CONSTRAINT_TYPE = 'P'
)
and OWNER = '&data_mart'


-- 6 Indexes

select index_type, count(*)
from dba_indexes
where owner = '&data_mart'
group by rollup (index_type)

-- B-tree indexes only pk indexes
select index_name
from dba_indexes
where 
owner = '&data_mart'
and index_type = 'NORMAL'
and index_name not in
(
   select index_name
    from dba_constraints
    where
        OWNER = '&data_mart'
        and CONSTRAINT_TYPE = 'P' and index_name is not null
)

-- check local indexes
select *
from dba_part_indexes
where owner = '&data_mart' and locality <> 'LOCAL'


-- 7 Stats Collections

-- find add partitions without stats collections afterwards

select *
from dba_source
where  regexp_like (text,'\s+ADD\s+PARTITION\s+','i')
and owner in ('ETL_DW', 'PERIF')
and name like '%SOC%';


select *
from dba_source
where upper(text) like '%GATHER%STATS%' and text not like '%WB_RT_MAPAUDIT_UTIL_INVOKER%'
and owner in ('ETL_DW', 'PERIF')
and name like '%SOC%';


------------------
select * from dba_segments where owner = '&data_mart' and segment_type like 'TABLE%'

select *
from dba_tables
where owner = '&data_mart'


select index_type, count(*)
from dba_indexes
where owner = '&data_mart'
group by index_type


select constraint_type, status, validated, rely, count(*) 
from dba_constraints
where OWNER = '&data_mart'
group by constraint_type, status, validated, rely


   select index_name
    from dba_constraints
    where
        OWNER = '&data_mart'
        and CONSTRAINT_TYPE = 'P'