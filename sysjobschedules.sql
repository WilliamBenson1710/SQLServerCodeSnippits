SELECT
@@SERVERNAME AS ServerName
,[schedule_id]
,[job_id]
,[next_run_date]
,[next_run_time]
FROM [msdb].[dbo].[sysjobschedules]
