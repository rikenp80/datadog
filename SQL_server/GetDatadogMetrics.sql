/****** Object:  StoredProcedure [dbo].[GetDatadogMetrics]    Script Date: 1/29/2019 15:50:09 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
  
CREATE PROCEDURE [dbo].[GetDatadogMetrics]
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;
/*#######################################################
CREATE TEMP TABLE to hold data to send to datadog
#######################################################*/
--Drop Table if exists
If OBJECT_ID('tempdb..#FileIOUsage') is not null DROP TABLE #FileIOUsage;
--CREATE TEMP TABLE
CREATE TABLE [#FileIOUsage](
    [Device] [varchar](2) NULL,
    [Database_Name] [nvarchar](128) NULL,
    [avg_read_stall_ms] [numeric](10, 1) NULL,
    [avg_write_stall_ms] [numeric](10, 1) NULL,
    [avg_io_stall_ms] [numeric](10, 1) NULL,
    [File_Size_MB] [decimal](18, 2) NULL,
    [physical_name] [nvarchar](260) NOT NULL,
    [type_desc] [nvarchar](60) NULL,
    [io_stall_read_ms] [bigint] NOT NULL,
    [num_of_reads] [bigint] NOT NULL,
    [io_stall_write_ms] [bigint] NOT NULL,
    [num_of_writes] [bigint] NOT NULL,
    [io_stalls] [bigint] NULL,
    [total_io] [bigint] NULL,
    [Resource_Governor_Total_Read_IO_Latency_ms] [bigint] NOT NULL,
    [Resource_Governor_Total_Write_IO_Latency_ms] [bigint] NOT NULL
)

/*#######################################################
Insert data on temporary table
#######################################################*/
 
INSERT into #FileIOUsage(Device, Database_Name, avg_read_stall_ms,avg_write_stall_ms,
avg_io_stall_ms,File_Size_MB,physical_name, type_desc,io_stall_read_ms, num_of_reads,io_stall_write_ms,
num_of_writes,io_stalls, total_io,Resource_Governor_Total_Read_IO_Latency_ms,Resource_Governor_Total_Write_IO_Latency_ms)
SELECT LEFT(mf.physical_name,1) as Device, DB_NAME(fs.database_id) AS [Database Name], CAST(fs.io_stall_read_ms/(1.0 + fs.num_of_reads) AS NUMERIC(10,1)) AS [avg_read_stall_ms],
CAST(fs.io_stall_write_ms/(1.0 + fs.num_of_writes) AS NUMERIC(10,1)) AS [avg_write_stall_ms],
CAST((fs.io_stall_read_ms + fs.io_stall_write_ms)/(1.0 + fs.num_of_reads + fs.num_of_writes) AS NUMERIC(10,1)) AS [avg_io_stall_ms],
CONVERT(DECIMAL(18,2), mf.size/128.0) AS [File Size (MB)],
--REPLACE(REPLACE(mf.physical_name,':','`:'),'\','`\'),
REVERSE(SUBSTRING(REVERSE(mf.physical_name),1,CHARINDEX('\',REVERSE(mf.physical_name))-1)) as physical_name,
mf.type_desc, fs.io_stall_read_ms, fs.num_of_reads,
fs.io_stall_write_ms, fs.num_of_writes, fs.io_stall_read_ms + fs.io_stall_write_ms AS [io_stalls], fs.num_of_reads + fs.num_of_writes AS [total_io],
io_stall_queued_read_ms AS [Resource Governor Total Read IO Latency (ms)], io_stall_queued_write_ms AS [Resource Governor Total Write IO Latency (ms)]
FROM sys.dm_io_virtual_file_stats(null,null) AS fs
INNER JOIN sys.master_files AS mf WITH (NOLOCK)
ON fs.database_id = mf.database_id
AND fs.[file_id] = mf.[file_id];


/*#######################################################
Create Table for metrics to be sent to Datadog
#######################################################*/
 --Drop table if exists
   IF Object_ID('tempdb..#Datadog') is not null DROP TABLE #Datadog;
 --Create Table
   CREATE TABLE #Datadog
    (
        [metric] varchar(255) not null,
         [type] varchar(50) not null,
         [value] float not null,
         [tags] varchar(255)
     )
/*#######################################################
Insert data on Datadog table
#######################################################*/
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.avg_read_stall_ms','gauge',avg_read_stall_ms,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.avg_write_stall_ms','gauge',avg_write_stall_ms,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.avg_io_stall_ms','gauge',avg_io_stall_ms,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.File_Size_MB','gauge',File_Size_MB,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.io_stall_read_ms','gauge',io_stall_read_ms,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.num_of_reads','gauge',num_of_reads,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.io_stall_write_ms','gauge',io_stall_write_ms,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.num_of_writes','gauge',num_of_writes,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.io_stalls','gauge',io_stalls,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.total_io','gauge',total_io,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.Resource_Governor_Total_Read_IO_Latency_ms','gauge',Resource_Governor_Total_Read_IO_Latency_ms,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.fileio.Resource_Governor_Total_Write_IO_Latency_ms','gauge',Resource_Governor_Total_Write_IO_Latency_ms,'sqlserver:'+@@SERVERNAME+',device:'+device+',file_type:'+[type_desc]+',database:'+Database_Name+',file:'+physical_name
FROM #FileIOUsage;


/*#######################################################
Gather Wait Statistics
#######################################################*/
If OBJECT_ID('tempdb..#Waits') is not null DROP TABLE #Waits;
WITH [Waits] AS
    (SELECT
        [wait_type],
        [wait_time_ms] / 1000.0 AS [WaitS],
        ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
        [signal_wait_time_ms] / 1000.0 AS [SignalS],
        [waiting_tasks_count] AS [WaitCount],
        100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
        ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
    FROM sys.dm_os_wait_stats
    WHERE [wait_type] NOT IN (
        -- These wait types are almost 100% never a problem and so they are
        -- filtered out to avoid them skewing the results. Click on the URL
        -- for more information.
        N'BROKER_EVENTHANDLER',
        N'BROKER_RECEIVE_WAITFOR',
        N'BROKER_TASK_STOP',
        N'BROKER_TO_FLUSH',
        N'BROKER_TRANSMITTER',
        N'CHKPT',
        N'CLR_AUTO_EVENT',
        N'CLR_MANUAL_EVENT',
        N'CLR_SEMAPHORE',
        N'CXCONSUMER',
  
        -- Maybe comment these four out if you have mirroring issues
        N'DBMIRROR_DBM_EVENT',
        N'DBMIRROR_EVENTS_QUEUE',
        N'DBMIRROR_WORKER_QUEUE',
        N'DBMIRRORING_CMD',
  
        N'DIRTY_PAGE_POLL',
        N'DISPATCHER_QUEUE_SEMAPHORE',
        N'EXECSYNC',
        N'FSAGENT',
        N'FT_IFTS_SCHEDULER_IDLE_WAIT',
        N'FT_IFTSHC_MUTEX',
  
        -- Maybe comment these six out if you have AG issues
        N'HADR_CLUSAPI_CALL',
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
        N'HADR_LOGCAPTURE_WAIT',
        N'HADR_NOTIFICATION_DEQUEUE',
        N'HADR_TIMER_TASK',
        N'HADR_WORK_QUEUE',
  
        N'KSOURCE_WAKEUP',
        N'LAZYWRITER_SLEEP',
        N'LOGMGR_QUEUE',
        N'MEMORY_ALLOCATION_EXT',
        N'ONDEMAND_TASK_QUEUE',
        N'PREEMPTIVE_XE_GETTARGETSTATE',
        N'PWAIT_ALL_COMPONENTS_INITIALIZED',
        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
        N'QDS_ASYNC_QUEUE',
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
        N'QDS_SHUTDOWN_QUEUE',
        N'REDO_THREAD_PENDING_WORK',
        N'REQUEST_FOR_DEADLOCK_SEARCH',
        N'RESOURCE_QUEUE',
        N'SERVER_IDLE_CHECK',
        N'SLEEP_BPOOL_FLUSH',
        N'SLEEP_DBSTARTUP',
        N'SLEEP_DCOMSTARTUP',
        N'SLEEP_MASTERDBREADY',
        N'SLEEP_MASTERMDREADY',
        N'SLEEP_MASTERUPGRADED',
        N'SLEEP_MSDBSTARTUP',
        N'SLEEP_SYSTEMTASK',
        N'SLEEP_TASK',
        N'SLEEP_TEMPDBSTARTUP',
        N'SNI_HTTP_ACCEPT',
        N'SP_SERVER_DIAGNOSTICS_SLEEP',
        N'SQLTRACE_BUFFER_FLUSH',
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        N'SQLTRACE_WAIT_ENTRIES',
        N'WAIT_FOR_RESULTS',
        N'WAITFOR',
        N'WAITFOR_TASKSHUTDOWN',
        N'WAIT_XTP_RECOVERY',
        N'WAIT_XTP_HOST_WAIT',
        N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
        N'WAIT_XTP_CKPT_CLOSE',
        N'XE_DISPATCHER_JOIN',
        N'XE_DISPATCHER_WAIT',
        N'XE_TIMER_EVENT'
        )
    AND [waiting_tasks_count] > 0
    )
SELECT
    MAX ([W1].[wait_type]) AS [WaitType],
    CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait_S],
    CAST (MAX ([W1].[ResourceS]) AS DECIMAL (16,2)) AS [Resource_S],
    CAST (MAX ([W1].[SignalS]) AS DECIMAL (16,2)) AS [Signal_S],
    MAX ([W1].[WaitCount]) AS [WaitCount],
    CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Percentage],
    CAST ((MAX ([W1].[WaitS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgWait_S],
    CAST ((MAX ([W1].[ResourceS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgRes_S],
    CAST ((MAX ([W1].[SignalS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgSig_S]
into #Waits
FROM [Waits] AS [W1]
INNER JOIN [Waits] AS [W2] ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX( [W1].[Percentage] ) < 95; -- percentage threshold
 
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.waits.resourcewait','gauge',AvgRes_S,'sqlserver:'+@@SERVERNAME+',WaitType:'+WaitType
FROM #Waits
Insert into #Datadog(metric,type, value,tags)
SELECT 'sqlserver.waits.cpuwait','gauge',AvgSig_S,'sqlserver:'+@@SERVERNAME+',WaitType:'+WaitType
FROM #Waits
 
 
/*#######################################################
SELECT statement for sending data to Datadog
If the distributor is in the server
#######################################################*/
Declare @dist_id int
Select @dist_id=database_id from sys.databases where name='distribution'
 
If @dist_id>1
BEGIN
Insert into replError(ID,[time],error_code,error_text,xact_seqno)
SELECT ID,[time],error_code,error_text,xact_seqno
FROM distribution.dbo.MSrepl_errors
where time not in (SELECT [time] from replError);
 
Insert into #Datadog (metric, type, value, tags)
Select 'sqlserver.repl.errortext','gauge', 1, 'sqlserver:'+@@SERVERNAME+'message:'+error_text from replError where sent=0 and error_text<>'' and error_text is not null
 
Update replError set sent=1
where sent=0
 
Declare @error_text varchar(100)
 
Select top 1 @error_text=error_text from replError order by time desc;
 
Declare @ex int
 
SELECT @ex=count(*)
FROM #Datadog
Where metric='sqlserver.repl.errortext'
 
IF @error_text='' or @ex=0
Insert into #Datadog (metric, type, value, tags)
Select 'sqlserver.repl.errortext','gauge', 0, 'sqlserver:'+@@SERVERNAME;
END
 
 
/*#######################################################
SELECT statement for sending data to Datadog
#######################################################*/
SELECT metric, type, value, tags from #Datadog
END
 
GO