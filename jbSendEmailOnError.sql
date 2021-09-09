DECLARE @JobID uniqueidentifier
, @JobStartTime VARCHAR(6)
, @JobStartDate VARCHAR(8);

SELECT @JobID = $(ESCAPE_NONE(JOBID))
, @JobStartDate = $(ESCAPE_NONE(STRTDT))
, @JobStartTime = $(ESCAPE_NONE(STRTTM));

EXEC [JobNotifications].[uspSendJobFailureWithSSISDDetail]
@JobUniqueId = @JobID
,@LastRunDate = @JobStartDate
,@LastRunTime = @JobStartTime
,@AdditionalRecipientsEmailAddress = 'jing.ma@dcyf.wa.gov;william.benson@dcyf.wa.gov';
