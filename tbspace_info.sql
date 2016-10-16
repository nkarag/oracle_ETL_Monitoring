-- find the tablespaces of each DWH Area
-- Staging
select distinct tablespace_name
from dba_tables
where owner in ('PRESTAGE_DW', 'PRESTAGE_PERIF', 'STAGE_DW', 'STAGE_PERIF','SHADOW_DW')

-- get the size of the "Staging" tablespaces
select sum(avail_MBs)
from (
SELECT dts.tablespace_name,NVL(ddf.bytes / 1024 / 1024, 0) avail_MBs,
    NVL(ddf.bytes - NVL(dfs.bytes, 0), 0)/1024/1024 used_MBs,
    NVL(dfs.bytes / 1024 / 1024, 0) free_MBs,
    TO_CHAR(NVL((ddf.bytes - NVL(dfs.bytes, 0)) / ddf.bytes * 100, 0), '990.00') "Used %",
    dts.contents,dts.extent_management,dts.status FROM sys.dba_tablespaces dts,
(select tablespace_name, sum(bytes) bytes
    from dba_data_files group by tablespace_name) ddf,
(select tablespace_name, sum(bytes) bytes
    from dba_free_space group by tablespace_name) dfs
WHERE dts.tablespace_name = ddf.tablespace_name(+)
    AND dts.tablespace_name = dfs.tablespace_name(+)
    AND NOT (dts.extent_management like 'LOCAL'
    AND dts.contents like 'TEMPORARY')
UNION ALL
SELECT dts.tablespace_name,NVL(dtf.bytes / 1024 / 1024, 0) avail,
    NVL(t.bytes, 0)/1024/1024 used,NVL(dtf.bytes - NVL(t.bytes, 0), 0)/1024/1024 free,
    TO_CHAR(NVL(t.bytes / dtf.bytes * 100, 0), '990.00') "Used %",dts.contents,
    dts.extent_management,dts.status
FROM sys.dba_tablespaces dts,
(select tablespace_name, sum(bytes) bytes
from dba_temp_files group by tablespace_name) dtf,
(select tablespace_name, sum(bytes_used) bytes
from v$temp_space_header group by tablespace_name) t
    WHERE dts.tablespace_name = dtf.tablespace_name(+)
    AND dts.tablespace_name = t.tablespace_name(+)
    AND dts.extent_management like 'LOCAL'
    AND dts.contents like 'TEMPORARY'
)t
where t.tablespace_name in (
select distinct tablespace_name
from dba_tables
where owner in ('PRESTAGE_DW', 'PRESTAGE_PERIF', 'STAGE_DW', 'STAGE_PERIF','SHADOW_DW')
)


-- get the size of the "DATA" tablespaces
select sum(avail_MBs)
from (
SELECT dts.tablespace_name,NVL(ddf.bytes / 1024 / 1024, 0) avail_MBs,
    NVL(ddf.bytes - NVL(dfs.bytes, 0), 0)/1024/1024 used_MBs,
    NVL(dfs.bytes / 1024 / 1024, 0) free_MBs,
    TO_CHAR(NVL((ddf.bytes - NVL(dfs.bytes, 0)) / ddf.bytes * 100, 0), '990.00') "Used %",
    dts.contents,dts.extent_management,dts.status FROM sys.dba_tablespaces dts,
(select tablespace_name, sum(bytes) bytes
    from dba_data_files group by tablespace_name) ddf,
(select tablespace_name, sum(bytes) bytes
    from dba_free_space group by tablespace_name) dfs
WHERE dts.tablespace_name = ddf.tablespace_name(+)
    AND dts.tablespace_name = dfs.tablespace_name(+)
    AND NOT (dts.extent_management like 'LOCAL'
    AND dts.contents like 'TEMPORARY')
UNION ALL
SELECT dts.tablespace_name,NVL(dtf.bytes / 1024 / 1024, 0) avail,
    NVL(t.bytes, 0)/1024/1024 used,NVL(dtf.bytes - NVL(t.bytes, 0), 0)/1024/1024 free,
    TO_CHAR(NVL(t.bytes / dtf.bytes * 100, 0), '990.00') "Used %",dts.contents,
    dts.extent_management,dts.status
FROM sys.dba_tablespaces dts,
(select tablespace_name, sum(bytes) bytes
from dba_temp_files group by tablespace_name) dtf,
(select tablespace_name, sum(bytes_used) bytes
from v$temp_space_header group by tablespace_name) t
    WHERE dts.tablespace_name = dtf.tablespace_name(+)
    AND dts.tablespace_name = t.tablespace_name(+)
    AND dts.extent_management like 'LOCAL'
    AND dts.contents like 'TEMPORARY'
)t
where t.tablespace_name in (
select distinct tablespace_name
from dba_tables
where owner not in ('PRESTAGE_DW', 'PRESTAGE_PERIF', 'STAGE_DW', 'STAGE_PERIF','SHADOW_DW')
)

-- get the size of the "redo logs" tablespaces
-- use EM and add the size of all redolog files:
-- 10redo_loggroups x 2files_per_group x 1GB per file = 20GBs

-- get the size of the "SUMMARY" tablespaces (MV, OLAP)
select sum(bytes)/1024/1024 MBs
from dba_segments t
where segment_name in (
select container_name
from dba_mviews
)