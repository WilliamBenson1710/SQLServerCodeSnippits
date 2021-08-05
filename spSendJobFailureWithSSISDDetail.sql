USE [DBMaint]
GO

/****** Object:  StoredProcedure [JobNotifications].[uspSendJobFailureWithSSISDDetail]    Script Date: 8/5/2021 10:17:02 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO









/*************************************************************************************************
*** OBJECT NAME                                                                                ***
**************************************************************************************************
[JobNotification].[spSendJobFailureWithSSISDDetail]

**************************************************************************************************
*** DESCRIPTION                                                                                ***
**************************************************************************************************
Pulls the error message for the job failure based on the job and excuation ids
Below informaotin is an example of how to use it in a SQL Server Job
DECLARE @JobID uniqueidentifier
, @JobStartTime VARCHAR(6)
, @JobStartDate VARCHAR(8)
;
SELECT @JobID = $(ESCAPE_NONE(JOBID))
, @JobStartDate = $(ESCAPE_NONE(STRTDT))
, @JobStartTime = $(ESCAPE_NONE(STRTTM))
;
EXEC [DBMaint].[JobNotifications].[spSendJobFailureWithSSISDDetail]
@JobUniqueId = JobID
,@LastRunDate = @JobStartDate
,@LastRunTime = @JobStartTime
**************************************************************************************************
*** CHANGE HISTORY                                                                             ***
**************************************************************************************************
2018-02-21: Created by william benson
2018-04-13: updated the the job history to look for sysj.run_time >= job start time  and < currnet time as int
2020-02-07: added the ability to send it to the service desk to the axosoft queue via the service desk

**************************************************************************************************
*** PERFORMANCE HISTORY                                                                        ***
**************************************************************************************************
Run Time = 0.1
 
**************************************************************************************************
*** TEST SCRIPT                                                                                ***
**************************************************************************************************
Example: To send an email based on a job id and specific run time
EXEC [JobNotifications].[spSendJobFailureWithSSISDDetail]
	@JobUniqueId = '775A3E4A-2C3E-4222-98F2-9FE898255722'
	,@Debug = 1
	,@LastRunDate = '20180221'
	,@LastRunTime = '153827'

Example: To send an email based on a job id and specific run time and see debug information
EXEC [JobNotifications].[spSendJobFailureWithSSISDDetail]
	@JobUniqueId = '6D76DA37-A71D-4113-B56A-AC2F3F95C812'
	,@LastRunDate = '20180221'
	,@LastRunTime = '153827'
	,@Debug = 1

Example: To send an email based on a job id and most recent job run and send a copy of the email to additional people
EXEC [JobNotification].[spSendJobFailureWithSSISDDetail]
	@JobUniqueId = '6D76DA37-A71D-4113-B56A-AC2F3F95C812'
	,@AdditionalRecipientsEmailAddress = 'John.Doe@SomeEmailAddress.com;Jane.Doe@SomeEmailAddress.com'

**************************************************************************************************/
CREATE PROCEDURE [JobNotifications].[uspSendJobFailureWithSSISDDetail]
	@JobUniqueId UNIQUEIDENTIFIER
	,@LastRunDate VARCHAR(10) = NULL
	,@LastRunTime VARCHAR(6) = NULL
	,@MailProfile NVARCHAR(250) = 'DCYF Database Server Notifications'
	,@AdditionalRecipientsEmailAddress AS VARCHAR(MAX) = ';'
	,@SendToServiceDesk BIT = 0
	,@ServiceDeskEmailAddress  NVARCHAR(250) = 'dcyf.servicedesk@dcyf.wa.gov'
	,@ServiceDeskDatabaseTeamQueue NVARCHAR(250) = 'Database Integration Team Q: '
	,@Debug BIT = 0

AS

BEGIN

	SET NOCOUNT ON;


	DECLARE
	@LastJobRunRequestedDate DATETIME
	,@NextRunTime AS INT
	,@NextRunDate AS INT
	,@NextJobRunDateTime DATETIME = NULL
	,@NextJobRunDate  DATETIME = NULL
	,@JobOwner AS VARCHAR(250)
	,@JobName VARCHAR(250)
	,@ServerName AS VARCHAR(50)
	,@JobHistoryRecordCount AS SMALLINT
	,@SSISDBHistoryRecordCount AS SMALLINT
	,@ServerType AS VARCHAR(50)
	,@Subject AS VARCHAR(250) --Subject of the email
	,@Message AS NVARCHAR(MAX) --Mesage body of the email
	,@JobOperatorEmail NVARCHAR(100)
	,@SendEmail BIT = 0
	,@CurrentTimeInt INT = 0
	,@M_ErrorMessage NVARCHAR(250)
	,@MailProfileToUse AS NVARCHAR(250);
	;

	/* Table variable to hold the history of the job */
	DECLARE @JobHisotryTable AS TABLE
	(
	StepId SMALLINT
	,StepName VARCHAR(250)
	,StepCommand VARCHAR(MAX)
	,HistoryMessage VARCHAR(MAX)
	,ExecutionId INT
	,SubSystem VARCHAR(50)
	) ;

	/* Table variable to hold the SSISDB history */
	DECLARE @SSISDBHistoryTable AS TABLE
	(
	SSISPackageName nvarchar(260)
	,SSISMessageSourceName nvarchar(4000)
	,SSISEventName nvarchar(1024)
	,SSISMessage nvarchar(max)
	,SSISOperationId BIGINT
	) ;

	/* Set the type of server for the email as well as the server name */ 
	/* this could be changed to use a configuration table on the server. */
	--Change this to use [DBMaint].[Lookup].[Server]
	SELECT @ServerType = CASE @@SERVERNAME WHEN 'DELOLYDB12007' THEN 'DEV ETL Server'
		WHEN 'DELOLYDB12008' THEN 'UAT ETL Server'
		WHEN 'DELOLYDB12009' THEN 'Prod ETL Server'
		ELSE 'DCYF SQL Server'
	END
	,@ServerName = @@SERVERNAME ;

    /*
    SELECT 
    svr.[ServerName]
    , env.EnvironmentName
    , clf.ClassificationName
    FROM [DBMaint].[Lookup].[Server] AS svr
    INNER JOIN [Lookup].[Environment] AS env
    ON svr.EnvironmentId = env.RecordId
    AND env.IsActive = 1
    INNER JOIN [Lookup].[Classification] AS clf
    ON clf.RecordId = svr.ClassificationId
    AND clf.IsActive = 1
    WHERE svr.ServerName = @@SERVERNAME
    AND svr.IsActive = 1
    */

	/* Set the current time to an int */
	SELECT @CurrentTimeInt = CONVERT(INT, REPLACE(CONVERT(VARCHAR(8), GETDATE(), 108),':',''));

	/* Get information about the job id */
	SELECT
	@JobName = sysj.[NAME]
	,@JobOwner = SUSER_SNAME(sysj.owner_sid)
	,@JobOperatorEmail = syso.email_address
	,@NextRunDate = syssc.next_run_date
	,@NextRunTime = syssc.next_run_time
	FROM msdb.dbo.sysjobs AS sysj
	LEFT OUTER JOIN msdb.dbo.sysjobschedules AS syssc
		ON sysj.job_id = syssc.job_id
	LEFT OUTER JOIN msdb.dbo.sysoperators AS syso
		ON sysj.notify_email_operator_id = syso.id
	WHERE sysj.job_id = @JobUniqueId;

	IF @Debug = 1
	BEGIN
		PRINT '------------ Debug Inoformation ----------------' ;
		PRINT '@JobUniqueId: ' + ISNULL(CAST(@JobUniqueId AS VARCHAR(100)), 'NULL') ;
		PRINT '@JobName: ' + ISNULL(CAST(@JobName AS VARCHAR(100)), 'NULL') ;
		PRINT '@JobOwner: ' + ISNULL(CAST(@JobOwner AS VARCHAR(100)), 'NULL') ;
		PRINT '@JobOperatorEmail: ' + ISNULL(CAST(@JobOperatorEmail AS VARCHAR(150)), 'NULL') ;             
		PRINT '@NextRunDate: ' + ISNULL(LEFT(CONVERT(VARCHAR, @NextRunDate, 120), 10), 'NULL') ;
		PRINT '@NextRunTime: ' + ISNULL(LEFT(CONVERT(VARCHAR, @NextRunTime, 108), 10), 'NULL') ;
		PRINT '@ServerType: ' + ISNULL(CAST(@ServerType AS VARCHAR(100)), 'NULL') ;
		PRINT '@ServerName: ' + ISNULL(CAST(@ServerName AS VARCHAR(100)), 'NULL') ;
	END

	/* If the job operator email is null than we set it to a defaul email address */
		/* this could be change to use a configuration table */
	IF @JobOperatorEmail IS NULL
	BEGIN
		SELECT @JobOperatorEmail = 'DCYFDLDatabaseServerNotifications@dcyf.wa.gov' --'DELDLSQLAdmins@del.wa.gov'

		IF @Debug = 1
			BEGIN
				PRINT '@JobOperatorEmail (Changed): ' + ISNULL(CAST(@JobOperatorEmail AS VARCHAR(100)), 'NULL') ;
			END
	END

	/* Get the last job run requested date if it was not pased in*/
	IF @LastRunDate IS NULL OR @LastRunTime IS NULL
	BEGIN
		SELECT
		@LastJobRunRequestedDate = MAX(RunDateTime)
		FROM (
			SELECT
			CONVERT(DATETIME, CONVERT(CHAR(8), run_date, 112) + ' ' + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), run_time), 6), 5, 0, ':'), 3, 0, ':')) AS RunDateTime
			FROM msdb.dbo.sysjobhistory
			WHERE job_id = @JobUniqueId
			AND run_status = 0
		) AS tblBase;

		SELECT @LastRunDate = CAST(CONVERT(VARCHAR(8), @LastJobRunRequestedDate, 112) AS INT)
		,@LastRunTime = REPLACE(CONVERT(VARCHAR(8), @LastJobRunRequestedDate, 108), ':', '')
	END

	IF @Debug = 1
		BEGIN
			PRINT '@LastJobRunRequestedDate: ' + ISNULL(LEFT(CONVERT(VARCHAR, @LastJobRunRequestedDate, 120), 20), 'NULL') ;
			PRINT '@LastRunDate: ' + ISNULL(LEFT(CONVERT(VARCHAR, @LastRunDate, 112), 8), 'NULL') ;
			PRINT '@LastRunTime: ' + ISNULL(LEFT(CONVERT(VARCHAR, @LastRunTime, 108), 8), 'NULL') ;
		END

	/*Insert job history information based off job id and dates passed in. Also we parse though the error message for the Execution ID
		to use in the SSIS DB query */
	INSERT INTO
	@JobHisotryTable
	(
	StepId
	,StepName
	,StepCommand
	,HistoryMessage
	,SubSystem
	,ExecutionId 
	)
	SELECT
	sysj.step_id
	, REPLACE(REPLACE(sysj.step_name, ')', ''), '(', '') AS step_name
	,syss.command
	, sysj.[message]
	, syss.subsystem
	, CASE WHEN CHARINDEX('dtsx' ,syss.command) > 0 AND syss.subsystem = 'SSIS'
		THEN [Utilities].[udfRemoveStringData](SUBSTRING(sysj.[message]
			, CHARINDEX('Execution ID:',sysj.[message])
			, CHARINDEX(', Execution Status:',sysj.[message]) - CHARINDEX('Execution ID:',sysj.[message]))
			)
		ELSE NULL
	END AS ExecutionId
	FROM msdb.dbo.sysjobhistory AS sysj
	LEFT OUTER JOIN msdb.dbo.sysjobsteps AS syss
		ON sysj.job_id = syss.job_id
		AND sysj.step_id = syss.step_id
	WHERE sysj.job_id = @JobUniqueId
		AND sysj.run_date = @LastRunDate
		AND sysj.run_time >= @LastRunTime
		AND sysj.run_time < @CurrentTimeInt
		AND sysj.run_status = 0;

	SET @JobHistoryRecordCount = @@ROWCOUNT ;

	IF @Debug = 1
	BEGIN
		PRINT '@JobHistoryRecordCount: ' + ISNULL(CAST(@JobHistoryRecordCount AS VARCHAR(100)), 'NULL') ;
		PRINT '@LastRunDate: ' + ISNULL(CAST(@LastRunDate AS VARCHAR(100)), 'NULL') ;
		PRINT '@LastRunTime: ' + ISNULL(CAST(@LastRunTime AS VARCHAR(100)), 'NULL') ;
		PRINT '@CurrentTimeInt: ' + ISNULL(CAST(@CurrentTimeInt AS VARCHAR(100)), 'NULL') ;
	END

	IF @JobHistoryRecordCount = 0
 
    BEGIN          
        SET @M_ErrorMessage = 'No job history found for job:' + @JobName + ' Run Date:' + CAST(@LastRunDate AS VARCHAR(8)) + ' Run Time:' +  CAST(@LastRunTime AS VARCHAR(6))
			+ ' Current Time:' +  CAST(@CurrentTimeInt AS VARCHAR(6));
        RAISERROR (@M_ErrorMessage, 18, 1) ;
    END

	/* Use the job history to pull any matching SSIS DB messages based off th Execution Id */
	INSERT INTO @SSISDBHistoryTable
	(
	SSISPackageName
	,SSISMessageSourceName
	,SSISEventName
	,SSISMessage
	,SSISOperationId)
	SELECT
	ssdb.package_name
	,ssdb.message_source_name
	,ssdb.event_name
	,ssdb.[message]
	,ssdb.operation_id
	FROM [SSISDB].[catalog].[event_messages] AS ssdb
	INNER JOIN @JobHisotryTable AS jht
		ON ssdb.operation_id = jht.ExecutionId
	WHERE [message_type] = 120

	SET @SSISDBHistoryRecordCount = @@ROWCOUNT;

	IF @Debug = 1
	BEGIN
		PRINT '@SSISDBHistoryRecordCount: ' + ISNULL(CAST(@SSISDBHistoryRecordCount AS VARCHAR(100)), 'NULL') ;
	END

	/* Start generatiing the email as long as we have history records */
	IF @JobHistoryRecordCount > 0 
	BEGIN 
                
		SET @Subject = @ServerType + ' || SQL Server Job Failure: ' + @JobName + ' on ' + @ServerName ;
            
		SET @Message = '<html>' + '<body style="font: 12px Arial;">'
			+ '<div id="intro2" style="width:670px;">The job ' + @JobName + ' failed on '
			+ @ServerName + '.' + '<br><br>' ;
            
		SET @Message = @Message + N'<div style="margin-top:10px; margin-left:0px; font:12px Arial">'
			+ N'Please take a moment to review the list of errors below'
			+ N'</div><div style="margin-top:10px;">'
			+ N'<table border="1" bordercolor=Black cellspacing="0" cellpadding="2" style="font:12px Arial">'
			+ N'<tr style="color:white;font-weight:bold;background-color:black;text-align:center">'
			+ N'<td>SQL Server Job Step Name</td>'
			+ N'<td>SQL Server Job Error Message</td>'
			+ N'<td>SSIS DB Execution Id</td>'
			+'</tr>'
			+ CAST((SELECT
					"td/@align" = 'CENTER'
					, td = [StepName]
					, ''
					, "td/@align" = 'LEFT'
					, td = [HistoryMessage]
					, ''
					, "td/@align" = 'CENTER'
					, td = COALESCE(CAST([ExecutionId] AS VARCHAR(10)), 'N/A')
					FROM
					@JobHisotryTable
					ORDER BY
					StepId
			FOR
					XML PATH('tr')
						, TYPE) AS NVARCHAR(MAX)) + N'</table></div>' ;


			IF @SSISDBHistoryRecordCount > 0
			BEGIN
			SET @Message = @Message + N'</div><br><div style="margin-top:10px;">'
			+ N'<table border="1" bordercolor=Black cellspacing="0" cellpadding="2" style="font:12px Arial">'
			+ N'<tr style="color:white;font-weight:bold;background-color:black;text-align:center">'
			+ N'<td>SSIS Operation Id</td>'
			+ N'<td>SSIS Package Name</td>'
			+ N'<td>SSIS Message Source Name</td>'
			+ N'<td>SSIS Event Name</td>'
			+ N'<td>SSIS Event Message</td>'
			+'</tr>'
			+ CAST((SELECT
					"td/@align" = 'CENTER'
					, td = CAST([SSISOperationId] AS VARCHAR(10))
					, ''
					, "td/@align" = 'CENTER'
					, td = [SSISPackageName]
					, ''
					, "td/@align" = 'CENTER'
					, td = [SSISMessageSourceName]
					, ''
					, "td/@align" = 'Center'
					, td = [SSISEventName]
					, ''
					, "td/@align" = 'LEFT'
					, td = SSISMessage
				FROM
				@SSISDBHistoryTable
				ORDER BY
				[SSISOperationId]
				FOR
				XML PATH('tr')
				, TYPE) AS NVARCHAR(MAX)) + N'</table></div>' ;
		END

		SET @Message = @Message
			--+ '</div>
			+ '<div id="notchanged" style="margin-top:10px; width:670px;"></div>'
			+ '<div style="margin-top:10px;"> '
			+ '<br>' + 'If you have any questions or concerns regarding this email, please feel free to contact your Database Administrators by hitting the Reply button</div>'
			--+ '<a href="mailto:DELDLDQLAdmins@del.wa.gov?Subject='+ @Subject + '"> the Database Administrators</a>.</div>'
			+ '<div style="margin-top:10px;">Sincerely,</div><div style="margin-top:10px;"></div>'
			+ '<div style="margin-top:10px;">The Database Administration Team</div><div style="margin-top:10px;"></div>'
			+ '</body></html>'
			--+ '<div id="disclaimer" style="margin-top:10px; font-weight:bold;">**** Please do not reply to this email. ****</div></div></div></body></html>' ; 


                
	
	/* Need to review how to get the next scheudle job run based on system schedule not activity 
	IF @NextJobRunDateTime IS NULL 
		BEGIN
			SET @Message = @Message
				+ '</div><br><div id="notchanged" style="margin-top:10px; width:670px;">'
				+ '<div style="margin-top:10px;"> ' + '<br/><br/>'
				+ 'If you have any questions or concerns regarding this email, please feel free to contact <a href="mailto:DELDLDQLAdmins@del.wa.gov?subject='
				+ @Subject + '">Your Friendly DEL Server Admins</a>.</div>'
				+ '<div style="margin-top:10px;">Thank You!</div><div style="margin-top:10px;"></div>'
				+ '<div id="disclaimer" style="margin-top:10px; font-weight:bold;">**** Please do not reply to this email. ****</div></div></div></body></html>' ;                           
		END
	ELSE 
		BEGIN 
                          
			SET @Message = @Message
				+ '</div><br><div id="notchanged" style="margin-top:10px; width:670px;">'
				+ '<div style="margin-top:10px;">The next time this job is scheduled to run is on '
				+ CONVERT(CHAR(10), @NextJobRunDateTime, 101) + ' at '
				--+ CONVERT(CHAR(10), @NextJobRunDate, 101) + '<br/><br/>'
				+ CONVERT(VARCHAR(8), @NextJobRunDateTime,108) + '<br/><br/>'
				+ dbTools.dbo.udfAMPMTimeFromDate(@NextJobRunDate) + '<br/><br/>'
				+ 'If you have any questions or concerns regarding this email, please feel free to contact '
				+ '<a href="mailto:DELDLDQLAdmins@del.wa.gov?subject='+ @Subject + '">Your Friendly DEL Server Admins</a>.</div>'
				+ '<div style="margin-top:10px;">Thank You!</div><div style="margin-top:10px;"></div>'
				+ '<div id="disclaimer" style="margin-top:10px; font-weight:bold;">**** Please do not reply to this email. ****</div></div></div></body></html>' ;
           
		END
	*/
	END 

	IF @Debug = 1
	BEGIN
		PRINT '@Subject: ' + ISNULL(CAST(@Subject AS VARCHAR(100)), 'NULL') ;
		PRINT '@Message: ' + ISNULL(CAST(@Message AS VARCHAR(MAX)), 'NULL') ;
		--PRINT '@NextJobRunDate: ' + ISNULL(CAST(@NextJobRunDate AS VARCHAR(100)), 'NULL') ;
		PRINT '@NextJobRunDate: ' + ISNULL(LEFT(CONVERT(VARCHAR, @NextJobRunDate, 120), 10), 'NULL') ;


		SELECT  @Message;
	END

	/* If no mail profile was passed in @MailProfileToUse then we set it to a defaul */ --this could be changed to a configuration table
	IF @MailProfileToUse IS NULL
	BEGIN

		EXEC [Utilities].[uspCheckMailProfileToUse]
		@MailProfileToSearchFor = @MailProfile
		,@ProfileNameToUse = @MailProfileToUse OUTPUT;
			
	END

	/* If the messase is not null and we have an operator email set the system to send the email */
	IF @Message IS NOT NULL AND @JobOperatorEmail IS NOT NULL
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
		    ,@recipients = @JobOperatorEmail 
			,@copy_recipients = @AdditionalRecipientsEmailAddress 
		    ,@subject = @Subject
			,@body = @Message
			,@body_format = 'HTML' 
			,@importance = 'High'
			;

	END

	IF @SendToServiceDesk = 1
	BEGIN
		
		SET @Subject = @ServiceDeskDatabaseTeamQueue + @ServerType + ' || SQL Server Job Failure: ' + @JobName + ' on ' + @ServerName ;

		EXEC msdb.dbo.sp_send_dbmail  
		    @profile_name = @MailProfileToUse
		    ,@recipients = @ServiceDeskEmailAddress 
			,@copy_recipients = @AdditionalRecipientsEmailAddress 
		    ,@subject = @Subject
			,@body = @Message
			,@body_format = 'HTML' 
			,@importance = 'High'
			;

	END


	IF @Debug = 1
	BEGIN
		PRINT '------------ End Inoformation ----------------' ;
	END
END
GO


