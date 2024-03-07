:: set root=C:\ProgramData\Anaconda3
:: call conda activate rdm_sql
:: set pyScript=E:\BinhNN2\ETL\main.py
:: python C:\ETL\scripts\hello.py
:: Denied set root=C:\Users\binhnn2\Anaconda3 

set root=C:\ProgramData\Anaconda3
:: set pyScript=E:\BinhNN2\ETL\main.py
set pyScript=C:\ETL\scripts\hello.py
call %root%\Scripts\activate.bat %root% 
call conda activate rdm_sql
E:
cd BinhNN2\ETL\scripts\
python %pyScript%


