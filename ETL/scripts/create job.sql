USE msdb;
GO

-- Create a new job
EXEC dbo.sp_add_job
    @job_name = N'ETL_IFRS9_Data_Job',
    @description = N'Job to execute ETL process for exchange rates';

-- Add a job step to run the Python script
EXEC sp_add_jobstep
    @job_name = N'ETL_IFRS9_Data_Job',
    @step_name = N'Run ETL Script',
    @subsystem = N'CmdExec',
    @command = N'python E:\BinhNN2\ETL\scripts\etl_exchange_rate.py';

-- Schedule the job to run daily at a specific time (e.g., 2:00 AM)
EXEC dbo.sp_add_schedule
    @schedule_name = N'ETL_Exchange_Rate_Schedule',
    @freq_type = 4,
    @freq_interval = 1,
    @active_start_time = 20000;

-- Attach the schedule to the job
EXEC dbo.sp_attach_schedule
    @job_name = N'ETL_IFRS9_Data_Job',
    @schedule_name = N'ETL_Exchange_Rate_Schedule';

-- Add the job to the SQL Server Agent
EXEC dbo.sp_add_jobserver
    @job_name = N'ETL_IFRS9_Data_Job',
    @server_name = N'(local)';