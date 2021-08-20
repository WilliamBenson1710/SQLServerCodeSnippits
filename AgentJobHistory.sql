SELECT
sysjh.server AS ServerName
,SUBSTRING(sysj.name,1,140) AS [JobName]
,sysjh.job_id AS JobGuid
,sysjh.step_id AS StepId
,sysjh.step_name AS StepName
--,sysjh.run_date
--,sysjh.run_time
,CASE WHEN sysjh.run_date > 0 THEN dbo.agent_datetime(sysjh.run_date, sysjh.run_time)
	ELSE NULL
END AS JobRunDatetime
,tbljbhist.next_scheduled_run_date AS NextRunDatetime
--, CASE WHEN sysjs.next_run_date > 0  THEN dbo.agent_datetime(sysjs.next_run_date, sysjs.next_run_time)
--ELSE NULL
--END AS NextRunDateTime
,sysjh.run_duration StepDuration
,CONVERT(NVARCHAR(50),CASE sysjh.run_status WHEN 0 THEN 'Failed'
	WHEN 1 THEN 'Succeeded'
	WHEN 2 THEN 'Retry'
	WHEN 3 THEN 'Cancelled'
	WHEN 4 THEN 'In Progress'
END
) AS ExecutionStatus
,sysjh.retries_attempted AS RetriesAttempted
,sysjh.sql_severity AS SQLSeverity
,sysjh.sql_message_id AS SQLMessageId
,sysjh.instance_id AS InstanceId
,sysjh.[message] AS JobHistoryMessage
FROM dbo.sysjobhistory AS sysjh

INNER JOIN dbo.sysjobs AS sysj
ON sysjh.job_id = sysj.job_id

LEFT OUTER JOIN (
	SELECT
	sysja.job_id
	,sysja.job_history_id
	,sysja.next_scheduled_run_date
	FROM dbo.sysjobactivity AS sysja
	INNER JOIN (
		SELECT job_id, MAX(session_id) AS SessionId FROM dbo.sysjobactivity GROUP BY job_id
	) AS lstses
	ON lstses.job_id = sysja.job_id
	AND sysja.session_id = lstses.SessionId
	WHERE sysja.next_scheduled_run_date IS NOT NULL
) AS tbljbhist
ON sysj.job_id = tbljbhist.job_id
AND sysjh.instance_id = tbljbhist.job_history_id

LEFT OUTER JOIN (
    SELECT
    ja.job_id,
    ja.start_execution_date    
    FROM msdb.dbo.sysjobactivity AS ja 
    LEFT JOIN msdb.dbo.sysjobhistory AS jh 
    ON ja.job_history_id = jh.instance_id
    WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
    AND ja.start_execution_date is not null
    AND ja.stop_execution_date is NULL
) AS tbkJobRunning
    ON sysj.job_id = tbkJobRunning.job_id
