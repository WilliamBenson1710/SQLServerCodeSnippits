USE [DBMaint]
GO
/****** Object:  StoredProcedure [BackupRestore].[RestoreLastDIFBackupFromAFileShare]    Script Date: 8/17/2021 12:46:02 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Charles Wolff
-- Create date: 12/12/2017
-- Description:	Restore the last full backup from a fileshare by checking the fileshare and finding the latest file
--				Please note, this currently doesn't do a with move statement.
-- =============================================
ALTER   PROCEDURE [BackupRestore].[RestoreLastDIFBackupFromAFileShare]
(
 @DatabasesToRestoreParam NVARCHAR(500)
,@FilePathParam VARCHAR(1000)
)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

---- To allow advanced options to be changed.
EXEC sp_configure 'show advanced options', 1;
---- To update the currently configured value for advanced options.
RECONFIGURE;

---- To enable the feature.
EXEC sp_configure 'xp_cmdshell', 1;

---- To update the currently configured value for this feature.
RECONFIGURE;

--DECLARE WORKING VARIABLES
Declare @FileName varChar(255)--For xp_dirtree
Declare @FileNameDIF varChar(255)--For xp_dirtree
--Declare @cmdText varChar(255) --For xp_dirtree
Declare @BKFolder varchar(255)--For xp_dirtree
DECLARE @NumberRecords INT --Number of records in the Database to restore table, used in While loop
DECLARE @RowCount	INT --Itterator for while loop
DECLARE @DatabaseName VARCHAR(250) --Not sure yet
--DECLARE @TruncateLog BIT = 0
DECLARE @sql VARCHAR(MAX) --Used to run the command to reset the Service Broker
DECLARE @InsertedRecords INT
DECLARE @InsertedRecordsDIF INT
DECLARE @cmdshell VARCHAR(1000)
DECLARE @cmdshellDIF VARCHAR(1000)
DECLARE @ErrorMessage NVARCHAR(4000)



--SET WORKING VARIABLES
SET @FileName = null
--set @cmdText = null

--CREATE NECESSARY TEMP TABLES
--create table #FileList (
--FileName varchar(255),
--DepthFlag int,
--FileFlag int
--)
CREATE TABLE #FileListFULL (
SrNo Int IDENTITY (1,1),
FileListOutPut Varchar(1000)
)

CREATE TABLE #FileListDIF (
SrNo Int IDENTITY (1,1),
FileListOutPut Varchar(1000)
)

CREATE TABLE #killspids
(
    rowid INT IDENTITY(1,1),
	dbid INT,
	spid SMALLINT
)

CREATE TABLE #RestoreTable
(
    rowid INT IDENTITY (1,1),
	databseName NVARCHAR(250),
)


CREATE TABLE #ServiceBrokerBefore
(
     DBID INT,
	dbName VARCHAR(50),
	brokerStatus BIT
)

CREATE TABLE #ServiceBrokerAfter
(
     RowNumber INT IDENTITY (1,1),
	 DBID INT,
	dbName VARCHAR(50),
	brokerStatus BIT
)

--INSERT VALUES INTO SERVICE BROKER BEFORE TABLE
INSERT INTO #ServiceBrokerBefore
        ( DBID, dbName, brokerStatus )
SELECT A.database_id,a.name, a.is_broker_enabled FROM sys.databases a

--INSTERT DATABASES TO RESTORE INTO #RESTORETABLE
INSERT INTO #RestoreTable
        ( databseName )

SELECT * FROM STRING_SPLIT(@DatabasesToRestoreParam,',')

--GET THE NUMBER OF RECORDS FROM #RESTORETABLE FOR WHILE LOOP ITTERATION
SET @NumberRecords = (SELECT MAX(rowid) FROM #RestoreTable)
SET @RowCount = (SELECT MIN(rowid) FROM #RestoreTable)

WHILE @RowCount <= @NumberRecords
	BEGIN
		--FILL THE @DATABASENAME VARIABLE
		SELECT @DatabaseName = databseName
			FROM #RestoreTable
			WHERE rowid = @RowCount

		--SET THE @BKFULDER VARIABLE FOR XP_DIRTREE
		set @BKFolder = @FilePathParam + @DatabaseName + '\'

		--check to make sure databse exists
			IF NOT EXISTS (SELECT name FROM master.sys.databases WHERE name = @databasename)
			BEGIN
				SET @ErrorMessage = 'One of the databases specified does not exist, please check your spelling and try again (' + @DatabaseName + ')'
				RAISERROR (@ErrorMessage, 16, 1, 0, 0)
				RETURN				

			END

				--get all the files and folders in the backup folder and put them in temporary table

				--This will set the command xp_cmdshell will run
				SET @cmdshell = 'DIR "' + @BKFolder + '*.bak" /OD /-C /B'	 
				SET @cmdshellDIF = 'DIR "' + @BKFolder + '*.dif" /OD /-C /B'
				-- /OD is the sort order by date/time (oldest first)
				-- /-C to disable display of seperator in the size value
				-- /B no heading information or summary
				TRUNCATE TABLE #FileListFULL
				TRUNCATE TABLE #FileListDIF
				--insert records from the desired folder into the temporary table
				INSERT INTO #FileListFULL (FileListOutPut)
				EXEC master.dbo.xp_cmdshell @cmdshell 		

				INSERT INTO #FileListDif (FileListOutPut)
				EXEC master.dbo.xp_cmdshell @cmdshellDIF	

				--get rid of NULL values			
				DELETE FROM #FileListFULL
				WHERE FileListOutPut IS NULL

				DELETE FROM #FileListDIF
				WHERE FileListOutPut IS NULL

				--Set the file name to be the newest FULL
				SELECT @InsertedRecords= MAX(SrNo) FROM #FileListFULL
				SELECT @InsertedRecordsDIF= MAX(SrNo) FROM #FileListdif	
				--get the latest backup file 
				SELECT @FileName = @BKFolder + FileListOutPut FROM #FileListFULL WHERE SrNo = @InsertedRecords
				SELECT @FileNameDIF = @BKFolder + FileListOutPut FROM #FileListDIF WHERE SrNo = @InsertedRecordsDIF  
				--select @filename

				--SELECT * FROM #FileListFULL
				--SELECT * FROM #FileListDIF

				--PRINT ('RESTORE DATABASE ' + @DatabaseName + ' FROM  DISK = ''' + @filename + '''  WITH NORECOVERY, FILE = 1, NOUNLOAD,  REPLACE,  STATS = 10')
				--PRINT ('RESTORE DATABASE ' + @DatabaseName + ' FROM  DISK = ''' + @filenameDIF + '''  WITH RECOVERY, FILE = 1, NOUNLOAD,  REPLACE,  STATS = 10')

				--Set the Database in Single User Mode
				EXEC('ALTER Database ' + @DatabaseName + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;')
				--EXECUTE THE RESTORE
				--select ('RESTORE DATABASE ' + @DatabaseName + ' FROM  DISK = ''' + @filename + '''  WITH  FILE = 1, NOUNLOAD,  REPLACE,  STATS = 10')
				EXEC('RESTORE DATABASE ' + @DatabaseName + ' FROM  DISK = ''' + @filename + '''  WITH NORECOVERY, FILE = 1, NOUNLOAD,  REPLACE,  STATS = 10')
				EXEC('RESTORE DATABASE ' + @DatabaseName + ' FROM  DISK = ''' + @filenameDIF + '''  WITH RECOVERY, FILE = 1, NOUNLOAD,  REPLACE,  STATS = 10')
				--Set the Database in Multi User Mode
				EXEC('ALTER DATABASE ' + @DatabaseName + ' SET MULTI_USER;')

				SET @RowCount = @RowCount + 1 
		END

		--GET SERVICE BROKER STATUS AFTER RESTORES ARE COMPLETE
		--CHECK FOR DIFFERENCE, AND THEN FIX THE DIFFERENCES
		INSERT INTO #ServiceBrokerAfter
        ( DBID, dbName, brokerStatus )
		SELECT A.database_id,a.name, a.is_broker_enabled 
		FROM sys.databases a
		INNER JOIN #ServiceBrokerBefore AS b
		ON a.database_id = b.DBID
		WHERE a.is_broker_enabled != b.brokerStatus

		SET @RowCount = (SELECT MIN(RowNumber) FROM #ServiceBrokerAfter)
		SET @NumberRecords = (SELECT MAX(RowNumber) FROM #ServiceBrokerAfter)

		WHILE @RowCount <= @NumberRecords
			BEGIN

				SELECT @DatabaseName = dbName
				FROM #ServiceBrokerAfter
				WHERE RowNumber = @RowCount

				SELECT @sql = 'ALTER DATABASE [' + @DatabaseName + '] SET ENABLE_BROKER;'

				EXEC (@sql)

			SET @RowCount = @RowCount + 1

		END

--CLEAN UP TEMP TABLES
DROP TABLE #FileListFULL
DROP TABLE #FileListDIF
DROP TABLE #killspids
DROP TABLE #RestoreTable
DROP TABLE #ServiceBrokerAfter
DROP TABLE #ServiceBrokerBefore


---- To allow advanced options to be changed.
EXEC sp_configure 'show advanced options', 1;
---- To update the currently configured value for advanced options.
RECONFIGURE;

---- To enable the feature.
EXEC sp_configure 'xp_cmdshell', 0;

---- To update the currently configured value for this feature.
RECONFIGURE;
END
