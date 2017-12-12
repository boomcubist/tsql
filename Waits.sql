12:10

	1. sys.dm_os_waiting_tasks
		
		SELECT
			[owt].[session_id],
			[owt].[exec_context_id],
			[owt].[wait_duration_ms],
			[owt].[wait_type],
			[owt].[blocking_session_id],
			[owt].[resource_description],
			[es].[program_name],
			[est].[text],
			[est].[dbid],
			[eqp].[query_plan],
			[es].[cpu_time],
			[es].[memory_usage]
		FROM sys.dm_os_waiting_tasks [owt]
			INNER JOIN sys.dm_exec_sessions [es] ON [owt].[session_id] = [es].[session_id]
			INNER JOIN sys.dm_exec_requests [er] ON [es].[session_id] = [er].[session_id]
			OUTER APPLY sys.dm_exec_sql_text ([er].[sql_handle]) [est]
			OUTER APPLY sys.dm_exec_query_plan ([er].[plan_handle]) [eqp]
		WHERE [es].[is_user_process] = 1
		ORDER BY [owt].[session_id], [owt].[exec_context_id]
		GO
		
	2. sys.dm_os_wait_stats 
		
		WITH [Waits] 
		AS (SELECT wait_type, wait_time_ms/ 1000.0 AS [WaitS],
		          (wait_time_ms - signal_wait_time_ms) / 1000.0 AS [ResourceS],
		           signal_wait_time_ms / 1000.0 AS [SignalS],
		           waiting_tasks_count AS [WaitCount],
		           100.0 *  wait_time_ms / SUM(wait_time_ms) OVER() AS [Percentage],
		           ROW_NUMBER() OVER(ORDER BY wait_time_ms DESC) AS [RowNum]
		    FROM sys.dm_os_wait_stats WITH (NOLOCK)
		    WHERE [wait_type] NOT IN (
		        N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP',
			   N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE',
		        N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE',
		        N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE',
			   N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE',
		        N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX',
		        N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', 
			   N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE',
		        N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'ONDEMAND_TASK_QUEUE',
		        N'PREEMPTIVE_OS_QUERYREGISTRY', 
			   N'PREEMPTIVE_HADR_LEASE_MECHANISM', N'PREEMPTIVE_SP_SERVER_DIAGNOSTICS',
			   N'PWAIT_ALL_COMPONENTS_INITIALIZED', 
			   N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
		        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'REQUEST_FOR_DEADLOCK_SEARCH',
			   N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP',
			   N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY',
		        N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK',
		        N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SP_SERVER_DIAGNOSTICS_SLEEP',
			   N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES',
			   N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_HOST_WAIT',
			   N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN',
		        N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT')
		    AND waiting_tasks_count > 0)
		SELECT
		    [WaitType]                     =    MAX(w1.wait_type),
		    [WaitPercentage]             =    CAST(MAX(w1.Percentage) AS DECIMAL(5,2)),
		    [AverageWaitTimeInSeconds]     =    CAST((MAX(w1.WaitS) / MAX(w1.WaitCount)) AS DECIMAL(16,4)),
		    [AverageResourceTimeInSeconds]     =    CAST((MAX(w1.ResourceS) / MAX(w1.WaitCount)) AS DECIMAL(16,4)),
		    [AverageSignalTimeInSeconds]     =    CAST((MAX(w1.SignalS) / MAX(w1.WaitCount)) AS DECIMAL(16,4)), 
		    [TotalWaitTimeInSeconds]         =    CAST(MAX(w1.WaitS) AS DECIMAL(16,2)),
		    [TotalResourceTimeInSeconds]     =    CAST(MAX(w1.ResourceS) AS DECIMAL(16,2)),
		    [TotalSignalTimeInSeconds]     =    CAST(MAX(w1.SignalS) AS DECIMAL(16,2)),
		    [NumberOfWaits]                 =    MAX(w1.WaitCount),
		    CAST (N'https://www.sqlskills.com/help/waits/' + w1.wait_type AS XML) AS [Help/Info URL]
		FROM Waits AS w1
		    INNER JOIN Waits AS w2 ON w2.RowNum <= w1.RowNum
		GROUP BY w1.RowNum, w1.wait_type
		HAVING SUM(w2.Percentage) - MAX(w1.Percentage) < 95 -- percentage threshold
		OPTION (RECOMPILE);
		
	3. Extended Events
	
		-- Drop the session if it exists. 
		IF EXISTS (
			SELECT * FROM sys.server_event_sessions
				WHERE [name] = N'MonitorWaits')
		    DROP EVENT SESSION [MonitorWaits] ON SERVER
		GO
		 
		 
		-- Create the event session
		CREATE EVENT SESSION [MonitorWaits] ON SERVER
		ADD EVENT [sqlos].[wait_info]
			(WHERE [sqlserver].[session_id] = 68 /*session_id of process or query*/)
		ADD TARGET [package0].[asynchronous_file_target]
		    (SET FILENAME = N'C:\Temp\EE_WaitStats.xel', 
		    METADATAFILE = N'C:\Temp\EE_WaitStats.xem')
		WITH (max_dispatch_latency = 1 seconds);
		GO
		 
		-- Start the session
		ALTER EVENT SESSION [MonitorWaits] ON SERVER
		STATE = START;
		GO
		 
		-- Go do the query
		 
		-- Stop the event session
		ALTER EVENT SESSION [MonitorWaits] ON SERVER
		STATE = STOP;
		GO
		 
		-- Do we have any rows yet?
		SELECT COUNT (*)
			FROM sys.fn_xe_file_target_read_file (
			'C:\Temp\EE_WaitStats*.xel',
			'C:\Temp\EE_WaitStats*.xem',
			null, null);
		GO
		 
		-- Create intermediate temp table for raw event data
		CREATE TABLE [##RawEventData] (
			[Rowid]            INT IDENTITY PRIMARY KEY,
			[event_data]    XML);
		GO
		 
		-- Read the file data into intermediate temp table
		INSERT INTO [##RawEventData] ([event_data])
		SELECT
		    CAST ([event_data] AS XML) AS [event_data]
		FROM sys.fn_xe_file_target_read_file (
			'C:\Temp\EE_WaitStats*.xel',
			'C:\Temp\EE_WaitStats*.xem',
			null, null);
		GO
		 
		-- And now extract everything nicely
		SELECT
			[event_data].[value] (
				'(/event/@timestamp)[1]',
					'DATETIME') AS [Time],
			[event_data].[value] (
				'(/event/data[@name=''wait_type'']/text)[1]',
					'VARCHAR(100)') AS [Wait Type],
			[event_data].[value] (
				'(/event/data[@name=''opcode'']/text)[1]',
					'VARCHAR(100)') AS [Op],
			[event_data].[value] (
				'(/event/data[@name=''duration'']/value)[1]',
					'BIGINT') AS [Duration (ms)],
			[event_data].[value] (
				'(/event/data[@name=''max_duration'']/value)[1]',
					'BIGINT') AS [Max Duration (ms)],
			[event_data].[value] (
				'(/event/data[@name=''total_duration'']/value)[1]',
					'BIGINT') AS [Total Duration (ms)],
			[event_data].[value] (
				'(/event/data[@name=''signal_duration'']/value)[1]',
					'BIGINT') AS [Signal Duration (ms)],
			[event_data].[value] (
				'(/event/data[@name=''completed_count'']/value)[1]',
					'BIGINT') AS [Count]
		FROM [##RawEventData];
		GO
		 
		-- And finally, aggregation
		SELECT
			[waits].[Wait Type],
			COUNT (*) AS [Wait Count],
			SUM ([waits].[Duration]) AS [Total Wait Time (ms)],
			SUM ([waits].[Duration]) - SUM ([waits].[Signal Duration])
				AS [Total Resource Wait Time (ms)],
			SUM ([waits].[Signal Duration]) AS [Total Signal Wait Time (ms)]
		FROM 
			(SELECT
				[event_data].[value] (
					'(/event/@timestamp)[1]',
						'DATETIME') AS [Time],
				[event_data].[value] (
					'(/event/data[@name=''wait_type'']/text)[1]',
						'VARCHAR(100)') AS [Wait Type],
				[event_data].[value] (
					'(/event/data[@name=''opcode'']/text)[1]',
						'VARCHAR(100)') AS [Op],
				[event_data].[value] (
					'(/event/data[@name=''duration'']/value)[1]',
						'BIGINT') AS [Duration],
				[event_data].[value] (
					'(/event/data[@name=''signal_duration'']/value)[1]',
						'BIGINT') AS [Signal Duration]
			FROM [##RawEventData]
			) AS [waits]
		WHERE [waits].[op] = 'End'
		GROUP BY [waits].[Wait Type]
		ORDER BY [Total Wait Time (ms)] DESC;
		GO
		 
		-- Cleanup
		DROP TABLE [##RawEventData];
		GO
		 
		IF EXISTS (
			SELECT * FROM sys.server_event_sessions
				WHERE [name] = N'MonitorWaits')
		    DROP EVENT SESSION [MonitorWaits] ON SERVER
GO