USE msdb;
GO

-- Add a new step to the existing job to run the etl_customer_info.py script
EXEC sp_add_jobstep
    @job_name = N'ETL_Exchange_Rate_Job',
    @step_name = N'Run ETL Customer Info Script',
    @subsystem = N'CmdExec',
    @command = N'python C:\path\to\your\script\etl_customer_info.py';

-- Attach the new step to the existing schedule
EXEC sp_attach_schedule
    @job_name = N'ETL_Exchange_Rate_Job',
    @schedule_name = N'ETL_Exchange_Rate_Schedule';
