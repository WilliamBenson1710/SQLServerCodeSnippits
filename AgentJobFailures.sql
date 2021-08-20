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
END AS FailureDateTime
,NULL AS NextRunDateTime
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
,sysjh.[message] AS ErrorMessage
FROM dbo.sysjobhistory AS sysjh

INNER JOIN dbo.sysjobs AS sysj
ON sysjh.job_id = sysj.job_id

--INNER JOIN dbo.sysjobschedules AS sysjs
--ON sysj.job_id = sysjs.job_id

WHERE sysjh.run_status NOT IN (1, 4)
AND sysjh.step_id != 0
