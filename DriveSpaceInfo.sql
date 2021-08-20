DECLARE @chkCMDShell AS SQL_VARIANT

EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;

SELECT @chkCMDShell = value FROM sys.configurations WHERE name = 'xp_cmdshell'
IF @chkCMDShell = 0
BEGIN
	EXEC sp_configure 'xp_cmdshell', 1
	RECONFIGURE;
END
--------------------------------------------------------------------------------------------
DECLARE @svrName VARCHAR(255)
DECLARE @sql VARCHAR(400)
SELECT @svrName = @@SERVERNAME

SELECT @sql = 'powershell.exe -c "Get-WmiObject -ComputerName ' + QUOTENAME(@svrName,'''') + ' -Class Win32_Volume -Filter ''DriveType = 3'' | select name,label,capacity,freespace | foreach{$_.name+''|''+$_.label+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''}"'

DECLARE @DriveSpaceTable AS TABLE(Line varchar(255)) -- #output

INSERT @DriveSpaceTable
EXEC xp_cmdshell @sql
;

SELECT @@SERVERNAME AS ServerName
	,REPLACE(RTRIM(LTRIM(SUBSTRING(line,1,CHARINDEX('|',line) -1))),':\','') AS OSDriveLetter
	,RTRIM(LTRIM(SUBSTRING(line,1,CHARINDEX('|',line) -1))) AS driveletter
	,SUBSTRING(line,CHARINDEX('|',line)+1,CASE WHEN (CHARINDEX('|',line,5) - CHARINDEX('|',line)) = 1 THEN NULL
		ELSE (CHARINDEX('|',line,5)  - CHARINDEX('|',line)-1) END) AS drivelabel
   ,ROUND(CAST(RTRIM(LTRIM(SUBSTRING(line,CHARINDEX('|',line,5)+1,(CHARINDEX('%',line) -1)-CHARINDEX('|',line,5)))) AS FLOAT)/1024,0) AS 'capacity(GB)'
   ,ROUND(CAST(RTRIM(LTRIM(SUBSTRING(line,CHARINDEX('%',line)+1,(CHARINDEX('*',line) -1)-CHARINDEX('%',line)))) AS FLOAT)/1024,0)AS 'freespace(GB)'
FROM @DriveSpaceTable
WHERE CHARINDEX('?',line) = 0

--------------------------------------------------------------------------------------------
DECLARE @chkCMDShell AS SQL_VARIANT

SELECT @chkCMDShell = value FROM sys.configurations WHERE name = 'xp_cmdshell'

IF @chkCMDShell = 1
BEGIN
EXEC sp_configure 'xp_cmdshell', 0
RECONFIGURE;
END

EXEC sp_configure 'show advanced options', 0;
RECONFIGURE;
