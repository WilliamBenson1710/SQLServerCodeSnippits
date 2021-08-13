DECLARE @chkCMDShell AS SQL_VARIANT
, @cmdshell NVARCHAR(1000)
, @FileRestorePath NVARCHAR(500) = '\\Dcyfolyut10015\etl\Production\DELOLYDB12009\Attendance\'
, @FullBackupName NVARCHAR(500) = NULL
, @DifferentialBackupName NVARCHAR(500) = NULL



CREATE TABLE #DirectoryFileList (
RecordId Int IDENTITY (1,1),
FileListOutPut Varchar(1000)
)

EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;

SELECT @chkCMDShell = value FROM sys.configurations WHERE name = 'xp_cmdshell'
IF @chkCMDShell = 0
BEGIN
EXEC sp_configure 'xp_cmdshell', 1
RECONFIGURE;
END
ELSE
BEGIN
PRINT 'xp_cmdshell is already enabled'
END
				
SET @cmdshell = 'DIR "' + @FileRestorePath + '*.bak" /O-D /-C /B'	 

-- /OD is the sort order by date/time (oldest first)
-- /O-D is the sort order by date/time (Newest First)
-- /-C to disable display of seperator in the size value
-- /B no heading information or summary

TRUNCATE TABLE #DirectoryFileList;

INSERT INTO #DirectoryFileList (FileListOutPut)
EXEC master.dbo.xp_cmdshell @cmdshell;

SELECT @FullBackupName = FileListOutPut FROM #DirectoryFileList WHERE RecordId = 1;

SET @cmdshell = 'DIR "' + @FileRestorePath + '*.dif" /O-D /-C /B'	

TRUNCATE TABLE #DirectoryFileList;

INSERT INTO #DirectoryFileList (FileListOutPut)
EXEC master.dbo.xp_cmdshell @cmdshell;

SELECT @DifferentialBackupName = FileListOutPut FROM #DirectoryFileList WHERE RecordId = 1;


/* Turn XP CMD Shell Off */
SELECT @chkCMDShell = value FROM sys.configurations WHERE name = 'xp_cmdshell'

IF @chkCMDShell = 1
BEGIN
EXEC sp_configure 'xp_cmdshell', 0
RECONFIGURE;
END
ELSE
BEGIN
PRINT 'xp_cmdshell is already enabled'
END

EXEC sp_configure 'show advanced options', 0;
RECONFIGURE;

SELECT
@FullBackupName AS FullBackupName
, @DifferentialBackupName AS DifferentialBackupName
