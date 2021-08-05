USE [DBMaint]
GO

/****** Object:  StoredProcedure [Detail].[uspDatabaseFileSizeNotifications]    Script Date: 8/5/2021 10:13:34 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*************************************************************************************************
*** OBJECT NAME                                                                                ***
**************************************************************************************************
[Detail].[uspDatabaseFileSizeNotifications]
**************************************************************************************************
*** DESCRIPTION                                                                                ***
**************************************************************************************************
-- Checks the Database file size log table and send notifications based on limits

**************************************************************************************************
*** CHANGE HISTORY                                                                             ***
**************************************************************************************************
2018-07-05 - Created by william.benson

**************************************************************************************************
*** PERFORMANCE HISTORY                                                                        ***
**************************************************************************************************
 
**************************************************************************************************
*** TEST SCRIPT                                                                                ***
**************************************************************************************************
USE DBMaint
GO

EXEC [Detail].[uspDatabaseFileSizeNotifications]
@Debug = 1

**************************************************************************************************/
CREATE PROCEDURE [Detail].[uspDatabaseFileSizeNotifications]
@TypeOfFileToCheck NVARCHAR(100) = 'LOG'
,@SendToEmailAddress AS NVARCHAR(250) = 'DCYFDLDatabaseServerNotifications@dcyf.wa.gov'
,@EmailAddressforReplys AS NVARCHAR(250) = 'DCYFDLDatabaseAdmins@dcyf.wa.gov'
,@MailProfileToUse NVARCHAR(100) = 'SQLAdmin'
,@FilterDaysToCheck SMALLINT = -2
,@GrowthPercentCheck DECIMAL(10,4) = 50.00
,@Debug BIT = 0

AS

BEGIN
	SET NOCOUNT ON;

	DECLARE @StartDate AS DATETIME
	,@DabaseFileSizeCount SMALLINT
	,@Subject AS NVARCHAR(250) --Subject of the email
	,@Message AS NVARCHAR(MAX) --Mesage body of the email
	,@ServerName AS VARCHAR(50)
	,@SendEmail BIT = 0
	;

	DECLARE @tblDabaseFileSizeDetail AS TABLE (ServerName NVARCHAR(250), DatabaseName NVARCHAR(250), [FileName] NVARCHAR(250), TypeOfFile NVARCHAR(20)
		, FileSizeGrowth DECIMAL(10,4), [FreePctMB] DECIMAL(10,2));

	SELECT @StartDate = CONVERT(CHAR(10), SYSDATETIME(),121)
	,@ServerName = @@SERVERNAME
	;

		IF @Debug = 1
	BEGIN
		PRINT '------------ Debug Inoformation ----------------' ;
		PRINT '@StartDate: ' + ISNULL(CAST(@StartDate AS VARCHAR(100)), 'NULL') ;	
		PRINT '@FilterDaysToCheck: ' + ISNULL(CAST(@FilterDaysToCheck AS VARCHAR(100)), 'NULL') ;	
		PRINT '@GrowthPercentCheck: ' + ISNULL(CAST(@GrowthPercentCheck AS VARCHAR(100)), 'NULL') ;	
		PRINT '@ServerName: ' + ISNULL(CAST(@ServerName AS VARCHAR(100)), 'NULL') ;	
		PRINT '@SendToEmailAddress: ' + ISNULL(CAST(@SendToEmailAddress AS VARCHAR(100)), 'NULL') ;	
		PRINT '@EmailAddressforReplys: ' + ISNULL(CAST(@EmailAddressforReplys AS VARCHAR(100)), 'NULL') ;	
		PRINT '@MailProfileToUse: ' + ISNULL(CAST(@MailProfileToUse AS VARCHAR(100)), 'NULL') ;	
	END

	;
	WITH cteLogFileGrowth AS (
	SELECT
	[RecordId]
	,[ServerName]
	,[DatabaseId]
	,[DatabaseName]
	,[FileId]
	,[FileName]
	,[TypeOfFile]
	,[PrevsDayFileSizeMB]
	,[FileSizeMB]
	,CASE WHEN [PrevsDayFileSizeMB] = 0 THEN 0.0
		WHEN [PrevsDayFileSizeMB] > 0 THEN (([FileSizeMB] - [PrevsDayFileSizeMB]) / [FileSizeMB]) * 100.00
		ELSE NULL
	END AS FileSizeGrowth
	,[FreePctMB]
	,[SizeCheckDatetime]
	FROM (
		SELECT
		[RecordId]
		,[ServerName]
		,[DatabaseId]
		,[DatabaseName]
		,[FileId]
		,[FileName]
		,[TypeOfFile]
		,[FileSizeMB]
		,LAG([FileSizeMB], 1,0) OVER (PARTITION BY [ServerName],[DatabaseName],[FileName] ORDER BY [SizeCheckDatetime])  AS PrevsDayFileSizeMB
		,CONVERT(DECIMAL(10,2),([FreeSpaceMB] / [FileSizeMB]) * 100.00) AS [FreePctMB]
		,[SizeCheckDatetime]
		,RANK() OVER (PARTITION BY [ServerName],[DatabaseName],[FileName] ORDER BY [SizeCheckDatetime] DESC) AS RecordRank
		FROM [DBMaint].[Detail].[DatabaseFileSizes]
		WHERE TypeOfFile = @TypeOfFileToCheck
			AND SizeCheckDatetime >= DATEADD(DAY, @FilterDaysToCheck, @StartDate)
		--AND DatabaseName IN ('Attendance')
		) AS tblDetailInfo
	WHERE tblDetailInfo.RecordRank = 1
	)
	INSERT INTO @tblDabaseFileSizeDetail(ServerName,DatabaseName,[FileName],[TypeOfFile],[FreePctMB],[FileSizeGrowth])
	SELECT
	ServerName
	,DatabaseName
	,[FileName]
	,TypeOfFile
	,FreePctMB
	,FileSizeGrowth
	FROM cteLogFileGrowth
	WHERE FileSizeGrowth >= @GrowthPercentCheck
	ORDER BY [ServerName],[DatabaseName],[SizeCheckDatetime] DESC

	SELECT @DabaseFileSizeCount = @@ROWCOUNT

	IF @Debug = 1
	BEGIN
		PRINT '@DabaseFileSizeCount: ' + ISNULL(CAST(@DabaseFileSizeCount AS VARCHAR(100)), 'NULL') ;
	END


	IF @DabaseFileSizeCount > 0
	BEGIN
		--SET @Subject = @ServerType + ' || SQL Server Job Failure: ' + @JobName + ' on ' + @ServerName ;
		SET @Subject = 'Database File Size Increase Report'
            
		--SET @Message = '<html>' + '<body style="font: 12px Arial;">'
		--	+ '<div id="intro2" style="width:670px;">The job ' + @JobName + ' failed on '
		--	+ @ServerName + '.' + '<br><br>' ;
            
		SET @Message = N'<div style="margin-top:10px; margin-left:0px; font:12px Arial">'
			+ N'Please take a moment to review the list of database files below'
			+ N'</div><div style="margin-top:10px;">'
			+ N'<table border="1" bordercolor=Black cellspacing="0" cellpadding="2" style="font:12px Arial">'
			+ N'<tr style="color:white;font-weight:bold;background-color:black;text-align:center">'
			+ N'<td>Server Name</td>'
			+ N'<td>Database Name</td>'
			+ N'<td>File Name</td>'
			+ N'<td>Type Of File</td>'
			+ N'<td>File Size Growth(MB)</td>'
			+ N'<td>Percent Free Space(MB)</td>'
			+'</tr>'
			+ CAST((SELECT
					"td/@align" = 'CENTER'
					, td = [ServerName]
					, ''
					, "td/@align" = 'CENTER'
					, td = [DatabaseName]
					, ''
					, "td/@align" = 'CENTER'
					, td = [FileName]
					, ''
					, "td/@align" = 'CENTER'
					, td = [TypeOfFile]
					, ''
					, "td/@align" = 'CENTER'
					, td = COALESCE(CAST([FileSizeGrowth] AS VARCHAR(10)), 'N/A')
					, ''
					, "td/@align" = 'CENTER'
					, td = COALESCE(CAST(FreePctMB AS VARCHAR(5)), 'N/A')
					FROM @tblDabaseFileSizeDetail
					ORDER BY ServerName,DatabaseName,[FileName],TypeOfFile,FileSizeGrowth DESC
					FOR
						XML PATH('tr')
							, TYPE) AS NVARCHAR(MAX)) 
			+ N'</table></div>';

		SET @Message = @Message
			--+ '</div>
			+ '<div id="notchanged" style="margin-top:10px; width:670px;"></div>'
			+ '<div style="margin-top:10px;"> '
			+ '<br>' + 'If you have any questions or concerns regarding this email, please feel free to contact your Database Administrators</div>'
			--+ '<a href="mailto:' + @EmailAddressforReplys + '?Subject='+ @Subject + '"> the Database Administrators</a>.</div>'
			+ '<div style="margin-top:10px;">Sincerely,</div><div style="margin-top:10px;"></div>'
			+ '<div style="margin-top:10px;">The Database Administration Team</div><div style="margin-top:10px;"></div>'
			+ '</body></html>'
			--+ '<div id="disclaimer" style="margin-top:10px; font-weight:bold;">**** Please do not reply to this email. ****</div></div></div></body></html>' ; 

	
			/* If no mail profile was passed in @MailProfileToUse then we set it to a defaul */ --this could be changed to a configuration table
		IF @MailProfileToUse IS NULL
		BEGIN
			SELECT @MailProfileToUse = 'SQLAdmin'
		END

		IF @Debug = 1
		BEGIN
			PRINT '@Subject: ' + ISNULL(CAST(@Subject AS VARCHAR(MAX)), 'NULL') ;
			PRINT '@Message: ' + ISNULL(CAST(@SendEmail AS VARCHAR(MAX)), 'NULL') ;
		END
			

		/* If the messase is not null and we have an operator email set the system to send the email */
		IF @Message IS NOT NULL AND @SendToEmailAddress IS NOT NULL
		BEGIN
			SELECT @SendEmail = 1
		END

		IF @Debug = 1
		BEGIN
			PRINT '@SendEmail: ' + ISNULL(CAST(@SendEmail AS VARCHAR(100)), 'NULL') ;
		END
			
		
		IF @SendEmail = 1
		BEGIN

			EXEC msdb.dbo.sp_send_dbmail  
				@profile_name = @MailProfileToUse
				,@recipients = @SendToEmailAddress
				,@subject = @Subject
				,@body = @Message
				,@body_format = 'HTML' 
				,@importance = 'High'
				;

		END
	
	END

	IF @Debug = 1
	BEGIN
		PRINT '------------ End Inoformation ------------------' ;
	END

END
GO


