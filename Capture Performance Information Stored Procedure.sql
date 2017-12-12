-- Create the schema first. 

USE [ServerMonitor]
GO
CREATE SCHEMA [performance] AUTHORIZATION [dbo]
GO


-- determine the SQL Server version


-- Typical resource bottlenecks
/*
Latencies caused by disk to memory transfers frequently surface as PageIOLatch waits.  Memory pressure or disk IO subsystem issues can also increase PageIOLatch waits. 
When a user needs a page that is not in buffer cache, SQL Server has to first allocate a buffer page, and then puts a exclusive PageIOLatch_ex latch on the buffer while the page is transferred from 
disk to cache. Meanwhile, SQL Server puts a PageIOLatch_sh request on the buffer on behalf of the user. After the write to cache finishes, the PageIOLatch_ex latch is released.  
This allows the user to read the buffer page after which the PageIOLatch_sh is released.  Consequently, high values for both PageIOLatch_ex and PageIOLatch_sh wait types can indicate IO subsystem issues.
Pertinent performance counters include Physical disk: disk seconds/read and Physical disk: disk seconds/write and SQL Server Buffer Manager: Page Life Expectancy. See counters for more information.

IO Stalls

The table valued dynamic management function, sys.dm_io_virtual_file_stats provides a breakdown of SQL Server reads, writes, and io_stalls for a particular database or transaction log file.  
IO_stalls is the total cumulative time, in milliseconds, that users waited for I/O to be completed on the file since the last restart of SQL Server. 
- Select * from sys.dm_io_virtual_file_stats (dbid,file#)
- Select * from sys.dm_io_virtual_file_stats (dbid,NULL) to list all files for a database. 

If IO_stalls is inordinately high for one or more files, it is possible that there is either a disk bottleneck or that high reads and writes are occurring on one drive. 
Average IO Waits per read or write can distinguish between consistently high IO queues or a temporary IO spike. 
A significantly higher average value for IO stalls on one particular drive indicates consistently high IO requests. 
This should be corroborated with Performance Monitor counters Physical Disk: Average Disk Seconds/Read and Average Disk Seconds/Write.  
*/

CREATE TABLE performance.IOStallsByFile
(
    CaptureID		    INT IDENTITY(1,1),
    CaptureDate	        DATETIME,
    DatabaseName	    VARCHAR(120),
    AvgReadStall_ms	    NUMERIC(10,1),
    AvgWriteStall_ms    NUMERIC(10,1),
    AvgIOStall_ms	    NUMERIC(10,1),
    FileSize_mb	        DECIMAL(18,2), 
    PhysicalName	    VARCHAR(1000),
    FileType		    VARCHAR(4), 
    IOStallRead_ms	    INT, 
    NumberOfReads	    INT, 
    IOStallWrite_ms	    INT, 
    NumberOfWrites	    INT, 
    IOStalls		    INT, 
    TotalIO		        INT
)


-- Calculates average stalls per read, per write, and per total input/output for each database file
INSERT performance.IOStallsByFile
SELECT 
    CaptureDate	        =   GETDATE(),
    DatabaseName	    =   DB_NAME(fs.database_id), 
    AvgReadStall_ms	    =   CAST(fs.io_stall_read_ms/(1.0 + fs.num_of_reads) AS NUMERIC(10,1)),
    AvgWriteStall_ms    =   CAST(fs.io_stall_write_ms/(1.0 + fs.num_of_writes) AS NUMERIC(10,1)),
    AvgIOStall_ms	    =   CAST((fs.io_stall_read_ms + fs.io_stall_write_ms)/(1.0 + fs.num_of_reads + fs.num_of_writes) AS NUMERIC(10,1)),
    FileSize_mb	        =   CONVERT(DECIMAL(18,2), mf.size/128.0), 
    PhysicalName	    =   mf.physical_name, 
    FileType		    =   mf.type_desc, 
    IOStallRead_ms	    =   fs.io_stall_read_ms, 
    NumberOfReads	    =   fs.num_of_reads, 
    IOStallWrite_ms	    =   fs.io_stall_write_ms, 
    NumberOfWrites	    =   fs.num_of_writes, 
    IOStalls		    =   fs.io_stall_read_ms + fs.io_stall_write_ms, 
    TotalIO		        =   fs.num_of_reads + fs.num_of_writes
FROM sys.dm_io_virtual_file_stats(null,null) AS fs
    INNER JOIN sys.master_files AS mf WITH (NOLOCK) ON fs.database_id = mf.database_id AND fs.[file_id] = mf.[file_id]
ORDER BY AvgIOStall_ms DESC OPTION (RECOMPILE);
------

-- Helps determine which database files on the entire instance have the most I/O bottlenecks
-- This can help you decide whether certain LUNs are overloaded and whether you might
-- want to move some files to a different location or perhaps improve your I/O performance
-- These latency numbers include all file activity against each SQL Server 
-- database file since SQL Server was last started


-- Query 2. 

/*
Missing or poorly formed indexes

Missing or poorly formed indexes can also cause excessive memory pressure or cache flushes. In some cases, SQL Server 2005 optimizer identifies potentially useful indexes to benefit a specific 
query (figure 1). The computed benefit of the index can be seen in the column avg_user_impact (percentage improvement with suggested index). It should be noted that this benefit applies to the 
individual query only where the maintenance cost is borne by inserts, updates, and delete operations. 
The following is a list of useful indexes.

*/
CREATE TABLE performance.MissingIndexes
(
    CaptureID		    INT IDENTITY(1,1),
    CaptureDate	    DATETIME,
    IndexAdvantage	    DECIMAL(18,2),
    LastUserSeek	    DATETIME,
    TableName		    VARCHAR(500),
    EqualityColumns	    VARCHAR(500),
    InequalityColumns   VARCHAR(500),
    IncludedColumns	    VARCHAR(500),
    UniqueCompiles	    INT,
    UserSeeks		    INT,
    AvgTotalUserCost    DECIMAL(18,2),
    AvgUserImpact	    DECIMAL(4,2)
)

-- Missing Indexes for all databases by Index Advantage (Missing Indexes All Databases)
INSERT performance.MissingIndexes
SELECT 
    CaptureDate	    =   GETDATE(),
    IndexAdvantage	    =   CONVERT(decimal(18,2),user_seeks * avg_total_user_cost * (avg_user_impact * 0.01)),
    LastUserSeek	    =   migs.last_user_seek, 
    TableName		    =   mid.[statement],
    EqualityColumns	    =   mid.equality_columns, 
    InequalityColumns   =   mid.inequality_columns, 
    IncludedColumns	    =   mid.included_columns,
    UniqueCompiles	    =   migs.unique_compiles, 
    UserSeeks		    =   migs.user_seeks, 
    AvgTotalUserCost    =   migs.avg_total_user_cost, 
    AvgUserImpact	    =   migs.avg_user_impact
FROM sys.dm_db_missing_index_group_stats AS migs WITH (NOLOCK)
    INNER JOIN sys.dm_db_missing_index_groups AS mig WITH (NOLOCK) ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details AS mid WITH (NOLOCK) ON mig.index_handle = mid.index_handle
ORDER BY IndexAdvantage DESC OPTION (RECOMPILE);
------

-- Getting missing index information for all of the databases on the instance is very useful
-- Look at last user seek time, number of user seeks to help determine source and importance
-- Also look at avg_user_impact and avg_total_user_cost to help determine importance
-- SQL Server is overly eager to add included columns, so beware
-- Do not just blindly add indexes that show up from this query!!!
-- to do.

CREATE TABLE performance.MissingIndexWarnings
(
    CaptureID		    INT IDENTITY(1,1),
    CaptureDate	    DATETIME,
    ObjectName		    VARCHAR(500),
    ObjectType		    VARCHAR(500),
    UseCount		    INT,
    Size_b		    INT,
    QueryPlan		    XML
)


-- Find missing index warnings for cached plans in the current database   (Missing Index Warnings)
-- Note: This query could take some time on a busy instance
SELECT TOP(25) 
    OBJECT_NAME(objectid) AS [ObjectName], 
    cp.objtype, 
    cp.usecounts, 
    cp.size_in_bytes, 
    query_plan
FROM sys.dm_exec_cached_plans AS cp WITH (NOLOCK)
    CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle) AS qp
WHERE CAST(query_plan AS NVARCHAR(MAX)) LIKE N'%MissingIndex%'
--AND dbid = DB_ID()
ORDER BY cp.usecounts DESC OPTION (RECOMPILE);

-- Helps you connect missing indexes to specific stored procedures or queries
-- This can help you decide whether to add them or not

CREATE TABLE performance.VolatileIndexes
(
    CaptureID		    INT IDENTITY(1,1),
    CaptureDate	    DATETIME    
)

-- Look at most frequently modified indexes and statistics (Volatile Indexes)
SELECT o.[name] AS [Object Name], o.[object_id], o.[type_desc], s.[name] AS [Statistics Name], 
       s.stats_id, s.no_recompute, s.auto_created, s.is_temporary,
	   sp.modification_counter, sp.[rows], sp.rows_sampled, sp.last_updated
FROM sys.objects AS o WITH (NOLOCK)
INNER JOIN sys.stats AS s WITH (NOLOCK)
ON s.object_id = o.object_id
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
WHERE o.[type_desc] NOT IN (N'SYSTEM_TABLE', N'INTERNAL_TABLE')
AND sp.modification_counter > 0
ORDER BY sp.modification_counter DESC, o.name OPTION (RECOMPILE);
------

-- This helps you understand your workload and make better decisions about 
-- things like data compression and adding new indexes to a table



-- Get fragmentation info for all indexes above a certain size in the current database   (Index Fragmentation)
-- Note: This query could take some time on a very large database
SELECT DB_NAME(ps.database_id) AS [Database Name], SCHEMA_NAME(o.[schema_id]) AS [Schema Name],
OBJECT_NAME(ps.OBJECT_ID) AS [Object Name], i.[name] AS [Index Name], ps.index_id, 
ps.index_type_desc, ps.avg_fragmentation_in_percent, 
ps.fragment_count, ps.page_count, i.fill_factor, i.has_filter, 
i.filter_definition, i.[allow_page_locks]
FROM sys.dm_db_index_physical_stats(DB_ID(),NULL, NULL, NULL , N'LIMITED') AS ps
INNER JOIN sys.indexes AS i WITH (NOLOCK)
ON ps.[object_id] = i.[object_id] 
AND ps.index_id = i.index_id
INNER JOIN sys.objects AS o WITH (NOLOCK)
ON i.[object_id] = o.[object_id]
WHERE ps.database_id = DB_ID()
AND ps.page_count > 2500
ORDER BY ps.avg_fragmentation_in_percent DESC OPTION (RECOMPILE);
------

-- Helps determine whether you have framentation in your relational indexes
-- and how effective your index maintenance strategy is




-- When were Statistics last updated on all indexes?   (Statistics Update)
SELECT 
    SCHEMA_NAME(o.Schema_ID) + N'.' + o.NAME AS [Object Name], 
    o.type_desc AS [Object Type],
    i.name AS [Index Name], 
    STATS_DATE(i.[object_id], i.index_id) AS [Statistics Date], 
    s.auto_created, 
    s.no_recompute, 
    s.user_created, 
    s.is_temporary,
    st.row_count, 
    st.used_page_count
FROM sys.objects AS o WITH (NOLOCK)
    INNER JOIN sys.indexes AS i WITH (NOLOCK) ON o.[object_id] = i.[object_id]
    INNER JOIN sys.stats AS s WITH (NOLOCK) ON i.[object_id] = s.[object_id] AND i.index_id = s.stats_id
    INNER JOIN sys.dm_db_partition_stats AS st WITH (NOLOCK) ON o.[object_id] = st.[object_id] AND i.[index_id] = st.[index_id]
WHERE o.[type] IN ('U', 'V')
AND st.row_count > 0
ORDER BY STATS_DATE(i.[object_id], i.index_id) DESC OPTION (RECOMPILE);
------  

-- Helps discover possible problems with out-of-date statistics
-- Also gives you an idea which indexes are the most active

-- sys.stats (Transact-SQL)
-- https://msdn.microsoft.com/en-us/library/ms177623.aspx
