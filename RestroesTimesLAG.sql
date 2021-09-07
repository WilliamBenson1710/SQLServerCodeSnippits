SELECT DISTINCT @@SERVERNAME AS ServerName,destination_database_name, 
                restore_date,
                LAG(restore_date,1,0) OVER (ORDER BY restore_date DESC, destination_database_name) AS PrevDBRestoreDate,
                CASE WHEN LAG(restore_date,1,0) OVER (ORDER BY restore_date DESC, destination_database_name) = '1900-01-01 00:00:00.000' THEN NULL
                    ELSE DATEDIFF(SECOND,restore_date,LAG(restore_date,1,0) OVER (ORDER BY restore_date DESC, destination_database_name)) 
                    END AS DiffSeconds,
                bs.backup_finish_date, 
                database_name        AS Source_database, 
                physical_device_name AS Backup_file_used_to_restore, 
                --LAG(restore_date,1,0) OVER (ORDER BY restore_date DESC, destination_database_name) AS TEST,
                LAG(database_name,1,0) OVER (ORDER BY restore_date DESC, destination_database_name) AS TEST1,
                bs.user_name, 
                bs.machine_name 
FROM   msdb.dbo.restorehistory rh 
       INNER JOIN msdb.dbo.backupset bs 
               ON rh.backup_set_id = bs.backup_set_id 
       INNER JOIN msdb.dbo.backupmediafamily bmf 
               ON bs.media_set_id = bmf.media_set_id 
WHERE  rh.restore_date >= '2021-09-02 11:30:11.190' --DATEADD(day, -1, Getdate()) 
--WHERE rh.destination_database_name = 'FRATS'
--ORDER  BY [rh].[restore_date] DESC 
ORDER BY restore_date DESC, destination_database_name
