SELECT 
CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(100)) AS [ServerName]
,CAST(@@SERVICENAME AS NVARCHAR(100)) AS [ServiceName]
,CONVERT(NVARCHAR(40), CASE LEFT(CONVERT(NVARCHAR(40), SERVERPROPERTY('ProductVersion')),4) 
   WHEN '8.00' THEN 'SQL Server 2000'
   WHEN '9.00' THEN 'SQL Server 2005'
   WHEN '10.0' THEN 'SQL Server 2008'
   WHEN '10.5' THEN 'SQL Server 2008 R2'
   WHEN '11.0' THEN 'SQL Server 2012'
   WHEN '12.0' THEN 'SQL Server 2014'
   WHEN '13.0' THEN 'SQL Server 2016'
   WHEN '14.0' THEN 'SQL Server 2017'
   WHEN '15.0' THEN 'SQL Server 2019'
   ELSE 'SQL Server 2019+'
END) AS [SQLVersionBuild]
,CAST(SERVERPROPERTY('Edition')AS NVARCHAR(100)) AS [Edition]
,CAST(@@VERSION AS NVARCHAR(1000)) AS SQLVersion
,LTRIM(RTRIM(SUBSTRING(@@Version, CHARINDEX('Windows',@@Version), CHARINDEX('<',@@Version) - CHARINDEX('Windows',@@Version)))) AS OSVersion
,CAST(SERVERPROPERTY('EngineEdition') AS NVARCHAR(100)) AS [EngineEdition]             
,CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(100)) AS [ProductVersion]
,(SELECT COUNT(1) FROM master.dbo.sysprocesses WHERE program_name = N'SQLAgent - Generic Refresher') AS SQLServerAgentRunning
,(SELECT sqlserver_start_time FROM sys.dm_os_sys_info) AS SQLServerStartTime
,(SELECT cpu_count FROM sys.dm_os_sys_info) AS [LogicalCPUCount]
,(SELECT hyperthread_ratio FROM sys.dm_os_sys_info) AS [HyperthreadRatio]
,(SELECT physical_memory_kb FROM sys.dm_os_sys_info) AS [PhysicalMemoryKB]
,(SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name LIKE '%Target Server%') AS TargetServerMemoryKB
,(SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name LIKE '%Total Server%') AS TotalUsedServerMemoryKB
