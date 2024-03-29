rem Set the root directory for the virtual environment
set root=C:\ProgramData\Anaconda3\Scripts

rem Activate the environment
call %root%\activate.bat

rem Activate the 'env_sql' environment
call conda activate env_sql

rem Set the path to the Python script
set pyScript=E:\BinhNN2\ETL\main.py

rem Change directory to the script directory for save log
cd /d E:\BinhNN2\ETL

rem Run the Python script
call python %pyScript%
