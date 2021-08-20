SELECT
@@ServerName AS ServerName
,bks.server_name AS BackupServerName
,bks.backup_set_id
,bks.media_set_id
,bks.[database_name]
,bks.backup_start_date
,bks.backup_finish_date
,DATEDIFF(MI, bks.backup_start_date, bks.backup_finish_date) AS DurationMin
,DATEDIFF(SS, bks.backup_start_date, bks.backup_finish_date) AS DurationSec
,bks.[type]
,CASE bks.[type] WHEN 'D' THEN 'Full Backup'
    WHEN 'I' THEN 'Differential Database'
    WHEN 'L' THEN 'Log'
    WHEN 'F' THEN 'File/Filegroup'
    WHEN 'G' THEN 'Differential File'
    WHEN 'P' THEN 'Partial' 
    WHEN 'Q' THEN 'Differential partial'
END AS BackupType
,CAST(ROUND(((bks.backup_size/1024)/1024),2) AS decimal(18,2)) AS BackupSizeMb
,CAST(ROUND(((bks.compressed_backup_size/1024)/1024),2) AS decimal(18,2)) AS CompressedBackupSizeMb
,has_bulk_logged_data
,is_snapshot
,is_readonly
,is_single_user
,has_backup_checksums
,is_damaged
,begins_log_chain
,is_force_offline
,bks.[user_name]
,CASE WHEN bks.database_name IN ('model','master','tempdb','msdb','SSISDB') THEN 1
    ELSE 0
END AS IsSystemDatabase
,rh.restore_date
,rh.restore_history_id
,rh.destination_database_name
,rh.restore_type
,rh.[replace]
,rh.[recovery]
,rh.[restart]
,CASE WHEN rh.restore_history_id IS NOT NULL THEN 1
    ELSE 0
END AS IsRestoreRecord
FROM msdb.dbo.backupset AS bks
LEFT OUTER JOIN (
	SELECT
	rsth.[restore_history_id]
	,rsth.[restore_date]
	,rsth.[destination_database_name] 
	,rsth.[backup_set_id]
	,rsth.[restore_type]
	,rsth.[replace]
	,rsth.[recovery]
	,rsth.[restart]
	FROM dbo.restorehistory AS rsth
	INNER JOIN (
	SELECT
	backup_set_id
	, MAX(restore_history_id) AS restore_history_id
	FROM dbo.restorehistory
	GROUP BY backup_set_id
	) AS tblMaxRestHist
	ON rsth.backup_set_id = tblMaxRestHist.backup_set_id
	AND rsth.restore_history_id = tblMaxRestHist.restore_history_id
) AS rh
    ON bks.backup_set_id = rh.backup_set_id
