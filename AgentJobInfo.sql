SELECT
@@SERVERNAME AS sql_server_name,
sysjobs.job_id AS sql_server_agent_job_id_guid,
sysjobs.name AS sql_server_agent_job_name,
sysjobs.date_created AS job_create_datetime_utc,
sysjobs.date_modified AS job_last_modified_datetime_utc,
sysjobs.enabled AS is_enabled,
0 AS is_deleted,
CONVERT(NVARCHAR(100), ISNULL(syscategories.name, '')) AS job_category_name
FROM msdb.dbo.sysjobs
LEFT JOIN msdb.dbo.syscategories
ON syscategories.category_id = sysjobs.category_id
