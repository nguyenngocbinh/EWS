USE [msdb]
GO

/****** Object:  Job [ETL_IFRS9_Data_Job_Copy]    Script Date: 7/10/2024 4:07:01 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 7/10/2024 4:07:01 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
    EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode = msdb.dbo.sp_add_job 
    @job_name=N'Run_Bscore_IFRS9', 
    @enabled=1, 
    @notify_level_eventlog=2, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Job to execute predict bscore lightgbm', 
    @category_name=N'[Uncategorized (Local)]', 
    @owner_login_name=N'NOR\BinhNN2', 
    @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

/****** Object:  Step [Run ETL Script]    Script Date: 7/10/2024 4:07:01 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
    @job_id=@jobId, 
    @step_name=N'Predict', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=1, 
    @on_success_step_id=0, 
    @on_fail_action=2, 
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, 
    @subsystem=N'TSQL', 
    @command=N'exec xp_cmdshell ''E:\BinhNN2\ifrs9PD\predict.bat''', 
    @database_name=N'master', 
    @flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
    @job_id=@jobId, 
    @step_name=N'Run monitor', 
    @step_id=2, 
    @cmdexec_success_code=0, 
    @on_success_action=1, 
    @on_success_step_id=0, 
    @on_fail_action=2, 
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, 
    @subsystem=N'TSQL', 
    @command=N'exec xp_cmdshell ''E:\BinhNN2\Monitor\monitor.bat''', 
    @database_name=N'master', 
    @flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_update_job 
    @job_id = @jobId, 
    @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback


DECLARE @ScheduleUID1 UNIQUEIDENTIFIER = NEWID()
DECLARE @ScheduleUID2 UNIQUEIDENTIFIER = NEWID()
DECLARE @ScheduleUID3 UNIQUEIDENTIFIER = NEWID()
DECLARE @ScheduleUID4 UNIQUEIDENTIFIER = NEWID()
DECLARE @ScheduleUID5 UNIQUEIDENTIFIER = NEWID()
DECLARE @ScheduleUID6 UNIQUEIDENTIFIER = NEWID()

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
    @job_id=@jobId, 
    @name=N'Monthly_10th', 
    @enabled=1, 
    @freq_type=16, 
    @freq_interval=10, 
    @freq_subday_type=1, 
    @freq_subday_interval=0, 
    @freq_relative_interval=10, 
    @freq_recurrence_factor=1, 
    @active_start_date=20240617, 
    @active_end_date=99991231, 
    @active_start_time=20000, 
    @active_end_time=235959, 
    @schedule_uid=@ScheduleUID1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
    @job_id=@jobId, 
    @name=N'Monthly_5th', 
    @enabled=1, 
    @freq_type=16, 
    @freq_interval=5, 
    @freq_subday_type=1, 
    @freq_subday_interval=0, 
    @freq_relative_interval=5, 
    @freq_recurrence_factor=1, 
    @active_start_date=20240617, 
    @active_end_date=99991231, 
    @active_start_time=20000, 
    @active_end_time=235959, 
    @schedule_uid=@ScheduleUID2
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
    @job_id=@jobId, 
    @name=N'Monthly_6th', 
    @enabled=1, 
    @freq_type=16, 
    @freq_interval=6, 
    @freq_subday_type=1, 
    @freq_subday_interval=0, 
    @freq_relative_interval=6, 
    @freq_recurrence_factor=1, 
    @active_start_date=20240617, 
    @active_end_date=99991231, 
    @active_start_time=20000, 
    @active_end_time=235959, 
    @schedule_uid=@ScheduleUID3
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
    @job_id=@jobId, 
    @name=N'Monthly_7th', 
    @enabled=1, 
    @freq_type=16, 
    @freq_interval=7, 
    @freq_subday_type=1, 
    @freq_subday_interval=0, 
    @freq_relative_interval=7, 
    @freq_recurrence_factor=1, 
    @active_start_date=20240617, 
    @active_end_date=99991231, 
    @active_start_time=20000, 
    @active_end_time=235959, 
    @schedule_uid=@ScheduleUID4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
    @job_id=@jobId, 
    @name=N'Monthly_8th', 
    @enabled=1, 
    @freq_type=16, 
    @freq_interval=8, 
    @freq_subday_type=1, 
    @freq_subday_interval=0, 
    @freq_relative_interval=8, 
    @freq_recurrence_factor=1, 
    @active_start_date=20240617, 
    @active_end_date=99991231, 
    @active_start_time=20000, 
    @active_end_time=235959, 
    @schedule_uid=@ScheduleUID5
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
    @job_id=@jobId, 
    @name=N'Monthly_9th', 
    @enabled=1, 
    @freq_type=16, 
    @freq_interval=9, 
    @freq_subday_type=1, 
    @freq_subday_interval=0, 
    @freq_relative_interval=9, 
    @freq_recurrence_factor=1, 
    @active_start_date=20240617, 
    @active_end_date=99991231, 
    @active_start_time=20000, 
    @active_end_time=235959, 
    @schedule_uid=@ScheduleUID6
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobserver 
    @job_id = @jobId, 
    @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

