/*
-- Enable xp_cmdshell if not already enabled
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'xp_cmdshell', 1;
RECONFIGURE;
*/

USE [msdb];
GO

-- Schedule the execution of the stored procedure using SQL Server Agent
DECLARE @jobId BINARY(16);
EXEC dbo.sp_add_job
    @job_name = N'ETL_IFRS9_Data_Job',
    @enabled = 1,
    @description = N'Job to execute ETL process for exchange rates',
    @job_id = @jobId OUTPUT;

EXEC sp_add_jobstep
    @job_name = N'ETL_IFRS9_Data_Job',
    @step_name = N'Run ETL Script',
    @subsystem = N'TSQL',
    @command = N"exec xp_cmdshell 'E:BinhNN2\ETL\scripts\etl.bat'";

EXEC dbo.sp_add_schedule
    @schedule_name = N'ETL_Exchange_Rate_Schedule',
    @freq_type = 4, -- Daily
    @freq_interval = 1, -- Every day
    @active_start_time = 20000; -- 2:00 AM

EXEC dbo.sp_attach_schedule
    @job_name = N'ETL_IFRS9_Data_Job',
    @schedule_name = N'ETL_Exchange_Rate_Schedule';

EXEC dbo.sp_add_jobserver
    @job_name = N'ETL_IFRS9_Data_Job',
    @server_name = N'(local)';