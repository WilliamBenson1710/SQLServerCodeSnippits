USE [DBMaint]
GO
/****** Object:  StoredProcedure [Detail].[uspDatabaseFileSizeGet]    Script Date: 8/5/2021 10:11:38 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*************************************************************************************************
*** OBJECT NAME                                                                                ***
**************************************************************************************************
[Detail].[uspDatabaseFileSizeGet]
**************************************************************************************************
*** DESCRIPTION                                                                                ***
**************************************************************************************************
--This procesure pulls a list of files sizes (Row/Log) and inserts the data into a table for tracking

**************************************************************************************************
*** CHANGE HISTORY                                                                             ***
**************************************************************************************************
2018-06-11: Created by William Benson (DEL)

**************************************************************************************************
*** PERFORMANCE HISTORY                                                                        ***
**************************************************************************************************
 
**************************************************************************************************
*** TEST SCRIPT                                                                                ***
**************************************************************************************************
USE DBMaint
GO

EXEC [Detail].[uspDatabaseFileSizeGet]
GO

**************************************************************************************************/
ALTER PROCEDURE [Detail].[uspDatabaseFileSizeGet]

AS

BEGIN

	SET NOCOUNT ON;

	/** Used for the details of the exception **/
	DECLARE @ErrMsg NVARCHAR(4000)
	, @ErrorMessage NVARCHAR(4000)
	, @ErrorSeverity SMALLINT
	, @E_DatabaseName VARCHAR(100)
	, @E_SchemaName VARCHAR(100)
	, @E_ProcedureName VARCHAR(255)
	, @E_ErrorLineNumber AS SMALLINT;

	BEGIN TRY

		INSERT INTO [Detail].[DatabaseFileSizes]
		([ServerName]
		,[DatabaseId]
		,[DatabaseName]
		,[FileId]
		,[FileName]
		,[TypeOfFile]
		,[PhysicalFileName]
		,[Size]
		,[MaxSize]
		,[FileSizeMB]
		,[SpaceUsedMB]
		,[FreeSpaceMB]
		,[SizeCheckDatetime])
		exec sp_msforeachdb 
		'use [?];
		SELECT
		@@SERVERNAME AS ServerName
		,DB_ID() AS DatabaseId  
		,DB_NAME() AS DatabaseName
		,[file_id] AS FileId
		,[Name] AS FileName
		,[type_desc] AS TypeOfFile
		,physical_name AS PhysicalName
		,SUM(SIZE) AS FileSize
		,SUM(MAX_SIZE) AS MaxSize
		,sum(size)/128.0 AS FileSizeMB 
		,SUM(CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT))/128.0 as SpaceUsedMB
		,SUM(size)/128.0 - sum(CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT))/128.0 AS FreeSpaceMB
		,GETDATE() AS SizeCheckDatetime  
		FROM sys.database_files 
		--where type=1
		GROUP BY [file_id], [name], [type_desc], [physical_name];'
		;

	END TRY
	BEGIN CATCH

		/** Grab specific information about the error/object **/
		SELECT @ErrorSeverity = ERROR_SEVERITY()
		, @ErrorMessage = ERROR_MESSAGE()
		, @E_DatabaseName = DB_NAME()
		, @E_SchemaName = OBJECT_SCHEMA_NAME(@@PROCID)
		, @E_ProcedureName = OBJECT_NAME(@@PROCID)
		, @E_ErrorLineNumber = ERROR_LINE();                   
                
		SET @ErrMsg = @ErrorMessage + ' Occurred at Line_Number: ' + CAST(ERROR_LINE() AS VARCHAR(50)) + ' (Severity ' + CAST(@ErrorSeverity AS VARCHAR) + ') '  
                                                                                                                                                     
		/** Raise the error message **/
		RAISERROR (@ErrMsg, 18, 1) ;

	END CATCH

END
