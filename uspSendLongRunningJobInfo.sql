USE [DBMaint]
GO

/****** Object:  StoredProcedure [JobNotifications].[uspSendLongRunningJobInfo]    Script Date: 8/5/2021 10:18:01 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



--EXEC [JobNotifications].[uspSendLongRunningJobInfo]SET QUOTED_IDENTIFIER ON
--SET ANSI_NULLS ON
--GO


/*************************************************************************************************
*** OBJECT NAME                                                                                ***
**************************************************************************************************
[JobNotifications].[spSendLongRunningJobInfo]

**************************************************************************************************
*** DESCRIPTION                                                                                ***
**************************************************************************************************
Variables:
@MinHistExecutions - Minimum number of job executions we want to consider 
@MinAvgSecsDuration - Threshold for minimum job duration we care to monitor
@HistoryStartDate - Start date for historical average
@HistoryEndDate - End date for historical average
 
These variables allow for us to control a couple of factors. First
we can focus on jobs that are running long enough on average for
us to be concerned with (say, 30 seconds or more). Second, we can
avoid being alerted by jobs that have run so few times that the
average and standard deviations are not quite stable yet. This script
leaves these variables at 1.0, but I would advise you alter them
upwards after testing.
 
Returns: One result set containing a list of jobs that
are currently running and are running longer than two standard deviations 
away from their historical average. The "Min Threshold" column
represents the average plus two standard deviations. 

note [1] - comment this line and note [2] line if you want to report on all history for jobs
note [2] - comment just this line is you want to report on running and non-running jobs
**************************************************************************************************
*** CHANGE HISTORY                                                                             ***
**************************************************************************************************
2018-12-08: Created by william benson
2021-03-04: William.Benson - Change the where clause to use AvgDuration so that it wont report out untill the job is actually past the average

**************************************************************************************************
*** PERFORMANCE HISTORY                                                                        ***
**************************************************************************************************
 
**************************************************************************************************
*** TEST SCRIPT                                                                                ***
**************************************************************************************************
USE DBMaint
GO

EXEC [JobNotifications].[spSendLongRunningJobInfo]
@HistoryStartDate = '2018-11-01'
, @HistoryEndDate = '2018-12-08'
, @MinHistExecutions = 6.0
, @MinAvgSecsDuration = 900.0
, @MailProfile = 'DCYF Database Server Notifications'
, @EmailRecipients = 'William.Benson@dcyf.wa.gov' --'dcyfdldatabaseservernotifications@dcyf.wa.gov'
**************************************************************************************************/
CREATE   PROCEDURE [JobNotifications].[uspSendLongRunningJobInfo]
@HistoryStartDate DATETIME 
  ,@HistoryEndDate DATETIME  
  ,@MinHistExecutions INT 
  ,@MinAvgSecsDuration INT
  ,@MailProfile NVARCHAR(250) = NULL
  ,@EmailRecipients NVARCHAR(MAX)
  ,@Debug BIT = 0

AS

BEGIN

	SET NOCOUNT ON;

	DECLARE
	@CountOfLongRunningJobs INT 
	,@Subject AS VARCHAR(250) --Subject of the email
	,@Message AS NVARCHAR(MAX) --Mesage body of the email
	,@SendEmail BIT = 0
	,@M_ErrorMessage NVARCHAR(250)
	,@ServerType AS VARCHAR(50)
	,@ServerName AS VARCHAR(50)
	,@MailProfileToUse AS NVARCHAR(250);
	;
 
    DECLARE @RunningJobs TABLE (
    [JobId] UNIQUEIDENTIFIER NOT NULL
    ,[ExecutionDate] DATETIME2(4) NOT NULL
    ,[CurrentExecutedStepId] TINYINT NULL
    ,[JobStepName] sysname NULL
    )

	DECLARE @LongRunningJobs TABLE (
	[JobId] UNIQUEIDENTIFIER NOT NULL
	,[JobName] NVARCHAR(250)
    ,[CurrentExecutionDate] DATETIME2(4) NULL
	,[ExecutionDate] DATETIME
	,[HistoricalAvgDurationSecs] DECIMAL(10,2)
	,[MinThreshholdSecs] DECIMAL(10,2)
	);

    INSERT INTO @RunningJobs
    (
        JobId
      , ExecutionDate
      , CurrentExecutedStepId
      , JobStepName
    )
    SELECT
    ja.job_id,
    ja.start_execution_date,      
    ISNULL(last_executed_step_id,0)+1 AS current_executed_step_id,
    Js.step_name
    FROM msdb.dbo.sysjobactivity ja 
    LEFT JOIN msdb.dbo.sysjobhistory jh 
    ON ja.job_history_id = jh.instance_id
    JOIN msdb.dbo.sysjobs j 
    ON ja.job_id = j.job_id
    JOIN msdb.dbo.sysjobsteps js
    ON ja.job_id = js.job_id
    AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
    WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
    AND start_execution_date is not null
    AND stop_execution_date is null;
 
	;WITH JobHistData AS
	(
	  SELECT job_id
	 ,date_executed = msdb.dbo.agent_datetime(run_date, run_time)
	 ,secs_duration = run_duration/10000*3600 + run_duration%10000/100*60 + run_duration%100
	  FROM msdb.dbo.sysjobhistory
	  WHERE step_id = 0   --Job Outcome
	  AND run_status = 1  --Succeeded
	)
	,JobHistStats AS
	(
	  SELECT
	  job_id
	  ,AvgDuration = AVG(secs_duration*1.)
	  ,AvgPlus2StDev = AVG(secs_duration*1.) + 2*stdevp(secs_duration)
	  FROM JobHistData
	  WHERE date_executed >= DATEADD(day, DATEDIFF(day,'19000101',@HistoryStartDate),'19000101')
	  AND date_executed < DATEADD(day, 1 + DATEDIFF(day,'19000101',@HistoryEndDate),'19000101') GROUP BY job_id HAVING COUNT(*) >= @MinHistExecutions
	  AND AVG(secs_duration*1.) >= @MinAvgSecsDuration
	)
	INSERT INTO @LongRunningJobs(
	[JobId]
	,[JobName]
    ,[CurrentExecutionDate]
	,[ExecutionDate]
	,[HistoricalAvgDurationSecs]
	,[MinThreshholdSecs]
	)
	SELECT
	jd.job_id
	,j.name AS [JobName]
    ,crj.ExecutionDate AS CurrentExecutionDate
	,MAX(act.start_execution_date) AS [ExecutionDate]
	,AvgDuration AS [HistoricalAvgDurationSecs]
	,AvgPlus2StDev AS [MinThreshholdSecs]
	FROM JobHistData jd
   	JOIN JobHistStats jhs
		ON jd.job_id = jhs.job_id
	JOIN msdb..sysjobs j 
		ON jd.job_id = j.job_id
	JOIN @RunningJobs crj
		ON crj.jobid = jd.job_id --see note [1] above
	LEFT OUTER JOIN msdb..sysjobactivity AS act
		ON act.job_id = jd.job_id
	--AND act.stop_execution_date IS NULL
	AND act.start_execution_date IS NOT NULL
	--WHERE DATEDIFF(SS, act.start_execution_date, GETDATE()) > AvgPlus2StDev
    WHERE DATEDIFF(SS, act.start_execution_date, GETDATE()) > AvgDuration
		--AND crj.job_state = 1 --see note [2] above
	GROUP BY jd.job_id, j.[name],crj.ExecutionDate
	, AvgDuration
	, AvgPlus2StDev
	;

	SELECT @CountOfLongRunningJobs = @@ROWCOUNT;

	IF @CountOfLongRunningJobs > 0
	BEGIN

		/* Set the type of server for the email as well as the server name */ 
		/* this could be changed to use a configuration table on the server. */
		SELECT @ServerType = CASE @@SERVERNAME WHEN 'DELOLYDB12007' THEN 'DEV ETL Server'
		WHEN 'DELOLYDB12008' THEN 'UAT ETL Server'
		WHEN 'DELOLYDB12009' THEN 'Prod ETL Server'
		ELSE @@SERVERNAME
		END
		,@ServerName = @@SERVERNAME ;
		
		
		SET @Subject = @ServerType + ' ||  Long Running Job Notifications on ' + @ServerName ;
            
		SET @Message = '<html>' + '<body style="font: 12px Arial;">'
			+ '<div id="intro2" style="width:670px;">Below is a list of long running jobs on on '
			+ @ServerName + '.' + '<br><br>' ;
            
		SET @Message = @Message + N'<div style="margin-top:10px; margin-left:0px; font:12px Arial">'
			+ N'Please take a moment to review the list of jobs that are running longer than expected'
			+ N'</div><div style="margin-top:10px;">'
			+ N'<table border="1" bordercolor=Black cellspacing="0" cellpadding="2" style="font:12px Arial">'
			+ N'<tr style="color:white;font-weight:bold;background-color:black;text-align:center">'
			+ N'<td>Job Name</td>'
			+ N'<td>Execution Date</td>'
			+ N'<td>Historical Avg Duration Secs</td>'
			+ N'<td>Min Threshhold Secs</td>'
			+ N'<td>Current Running Time</td>'
			+'</tr>'
			+ CAST((SELECT
					"td/@align" = 'CENTER'
					, td = [JobName]
					, ''
					, "td/@align" = 'Center'
					, td = FORMAT ([CurrentExecutionDate], 'MM/dd/yyyy hh:mm:ss tt') --FORMAT(ExecutionDate, 'd', 'en-US' )FORMAT(CAST(ExecutionDate AS DATETIME), 'MM/dd/yyy') --CONVERT(DATETIME, ExecutionDate,20)
					, ''
					, "td/@align" = 'Center'
					, td = ETLLog.Utilities.udfFormatTime([HistoricalAvgDurationSecs], 'S', '%D% Day %H% Hr %M% Min %S% Sec')
					, ''
					, "td/@align" = 'Center' --[MinThreshholdSecs]
					, td = ETLLog.Utilities.udfFormatTime([MinThreshholdSecs], 'S', '%D% Day %H% Hr %M% Min %S% Sec')
                    , ''
					, "td/@align" = 'Center' --[MinThreshholdSecs]
					, td = ETLLog.Utilities.udfFormatTime(DATEDIFF(SECOND,[CurrentExecutionDate],SYSDATETIME()), 'S', '%D% Day %H% Hr %M% Min %S% Sec')
					FROM
					@LongRunningJobs
					ORDER BY
					[JobName]
			FOR
					XML PATH('tr')
						, TYPE) AS NVARCHAR(MAX)) + N'</table></div>' ;			

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

	END

	IF @Message IS NOT NULL AND @Subject IS NOT NULL AND @EmailRecipients IS NOT NULL
	BEGIN
		SELECT @SendEmail = 1
	END

	IF @SendEmail = 1
	BEGIN

		EXEC [Utilities].[uspCheckMailProfileToUse]
		@MailProfileToSearchFor = @MailProfile
		,@ProfileNameToUse = @MailProfileToUse OUTPUT;

		EXEC msdb.dbo.sp_send_dbmail  
		    @profile_name = @MailProfileToUse
		    ,@recipients = @EmailRecipients 
			--,@copy_recipients = @AdditionalRecipientsEmailAddress 
		    ,@subject = @Subject
			,@body = @Message
			,@body_format = 'HTML' 
			,@importance = 'High'
			;

	END

END
GO


