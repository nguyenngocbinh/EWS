/*
	Author: BINHNN2
	Purpose: Chuan bi 32 truong du lieu EWS
	Start date: 30/09/2021
	Prefix table: EWS
	Update: 04/10/2021: 
	- Them current_default
	- max_dpd -> dpd (max_dpd trong thang chua test)
	Update 07/10/2021: 
	- chay so lieu thang 9
	- Sua Mob
	Update 02/11/2021: Chay so lieu thang 10
	Update 18/11/2021: Sua tien to IFRS9 thanh EWS
	Update 29/11/2021: 
	- Sua dinh dang tu numeric -> float
	- day_to_matdt chi tinh voi CL
	Update 02/12/2021: Fix trung bang contract
	Update 06/12/2021: thay doi and -> or -> current default
	Update 08/12/2021: them truong [os_amount_by_portfolio]
	Update 14/12/2021: Doi ten bang IFRS9_CTR_GL23 -> EWS_CTR_GL23
	Update 16/12/2021: ca_bal_l3m: tinh lai, truoc lay so tu mart
	Update 04/01/2022:
		- them DPD thau chi tu mis035b
		- bo rule loai thau chi dau 2, 3
		- them rule loai branch_code = 555
	Update 06/01/2022: 
		- them branch_code de loc khach hang cho ds
		- loai khach CL tat toan trong thang
		- Mob khong tinh LC
	Update 21/01/2022:
		Quy doi: outstanding_avg_month, os_amount
	Update 25/01/2022: thay doi voi du lieu thang 12
	,x.avg_cash_txn_amt_per_time_l3m = case when y.NUM_CASH_TXN <> 0 then y.CASH_TXN_AMT_L3M/y.NUM_CASH_TXN end
	update: 27/01/2022: current_default, nhom_no_contract the
	Update: 2022-04-15 them phan tinh pd, score, grade khach hang
			Chinh sua lai phan store bang cuoi cung
	Update: 2022-04-19: 
	x.avg_cash_txn_amt_per_time_l3m = case when y.NUM_NON_CASH_TXN <> 0 then y.CASH_TXN_AMT_L3M/y.NUM_NON_CASH_TXN end 
	Update: 2022-05-17 them grade_num
	Update: 2022-10-06: 
		- thay doi inner_join product_code thanh left_join
		- thay doi product_code tu thang 9/2023
		- isnull(max_dpd, 0) trong phan bucket
		- Cac bang co dinh chuyen thanh bang tam
	Update: 2023-02-23 (bat dau tu 2023-01-31 chay theo code moi nay)
		- Thay bang IFRS9_CTR_CC thanh [dbo].[IFRS9_CTR_CC_VISA028]
		- Thay bang IFRS9_CTR_CL thanh [dbo].[IFRS9_CTR_CL_CL020]
		- Tinh mob va current_default theo theo bang CUST_ODR_LRA cua Huyen
	Update: 2023-09-21:
		- Bang product_code update 1 so ma san pham moi 
		- Bang master scale thay bang master_scale_portfolio
		- Bang KHDN update bucket, mob, dpd theo bang CUST_ODR_LRA cua Huyen
		- Update grade_num = 13 cho grade Default
*/
use ews
go
/* check available variable
select *
from information_schema.columns
where lower(column_name) in (
		'dpd_l6m'
		,'dpd_l3m'
		,'min_ca_bal_l6m'
		,'avg_total_num_txn_l6m'
		,'avg_total_num_txn'
		,'avg_total_num_txn_l3m'
		,'avg_cr_txn_amt_vs_os_l6m'
		,'avg_txn_amt_os_onbs_l3m'
		,'vol_os_l6m'
		,'remain_term_autoloan'
		,'max_os_onbs_l6m'
		,'avg_bal_onbs_l6m'
		,'remain_term'
		,'total_bal_avg'
		,'ca_bal_os_onbs_avg'
		,'total_num_txn_l6m'
		,'avg_bal_l3m'
		,'avg_bal_l6m'
		,'ca_bal_os_onbs_l3m_avg'
		,'bal_l6m'
		,'os_amt_long_curr'
		,'total_num_txn_l3m'
		,'max_net_flow_txn_amt_l3m'
		,'ca_bal_l3m'
		,'max_dpd_loan'
		,'total_num_txn'
		,'os_revl_l3m'
		,'os_revl'
		,'ca_bal'
		,'min_day_to_matdt'
		,'num_cr_product_l3m'
		,'ca_td_bal_legacy'
		)
*/
-------------------------------------------------------------------------------------
-- Form bang dung de tong hop thong tin theo cap do khach hang
drop table if exists ##ews_shortlist;
create table ##ews_shortlist (
	process_date [date] null
	,customer_id [nvarchar](50) null
	,customer_type [nvarchar](50) null
	,cg_segment [nvarchar](50) null
	,ifrs9_segment [nvarchar](50) null
	,current_default [float] null	
	,mob [float] null
	,max_dpd [float] null
	,dpd_l6m [float] null
	,dpd_l3m [float] null
	,min_ca_bal_l6m [float] null
	,avg_total_num_txn_l6m [float] null
	,avg_total_num_txn [float] null
	,avg_total_num_txn_l3m [float] null
	,avg_cr_txn_amt_vs_os_l6m [float] null
	,avg_txn_amt_os_onbs_l3m [float] null
	,vol_os_l6m [float] null
	,remain_term_autoloan [float] null
	,max_os_onbs_l6m [float] null
	,avg_bal_onbs_l6m [float] null
	,remain_term [float] null
	,total_bal_avg [float] null
	,ca_bal_os_onbs_avg [float] null
	,total_num_txn_l6m [float] null
	,avg_bal_l3m [float] null
	,avg_bal_l6m [float] null
	,ca_bal_os_onbs_l3m_avg [float] null
	,bal_l6m [float] null
	,os_amt_long_curr [float] null
	,total_num_txn_l3m [float] null
	,max_net_flow_txn_amt_l3m [float] null
	,ca_bal_l3m [float] null
	,max_dpd_loan [float] null
	,total_num_txn [float] null
	,os_revl_l3m [float] null
	,os_revl [float] null
	,ca_bal [float] null
	,min_day_to_matdt [float] null
	,num_cr_product_l3m [float] null
	,ca_td_bal_legacy [float] null
	,os_amount_by_portfolio [float] null
	,total_bal_os_onbs_avg [float] null
	,num_xdpd_l6m [float] null	
	,avg_os_onbs [float] null
	,avg_ebank_txn_amt_l3m [float] null
	,avg_cash_txn_amt_per_time_l3m [float] null
	,os_amount [float] null
	,branch_code [nvarchar](50) null
	);

declare @run_date  date = '2023-11-30';
-- Chi lay dau GL = 2, 3 voi OD va CL
drop table if exists ##EWS_CTR_GL23;

with tbl as (
select process_date
	,mud_code
	,BRANCH_CODE
	,contract_id
	,customer_id
	,cus_segment
	,customer_industry
	,product_code
	,currency
	,disbursement_date
	,expiry_date
	,interest_rate
	,os_amount	
	,dpd
	,nhom_no_contract
	,ovd_prin_start_date
	,ovd_int_start_date
	,outstanding_avg_month
	,EXR_AT_DISBURSEMENT
from EWS..IFRS9_CTR_CC_VISA028
union
select process_date
	,mud_code
	,BRANCH_CODE
	,contract_id
	,customer_id
	,cus_segment
	,customer_industry
	,product_code
	,currency
	,disbursement_date
	,expiry_date
	,interest_rate
	,os_amount	
	,dpd
	,nhom_no_contract
	,ovd_prin_start_date
	,ovd_int_start_date
	,outstanding_avg_month
	,EXR_AT_DISBURSEMENT
from EWS.[dbo].[IFRS9_CTR_CL_CL020]
where SUBSTRING(convert(varchar,[GL_CODE]), 1, 1) in ('2', '3')
union
(
select process_date
	,mud_code
	,BRANCH_CODE
	,contract_id
	,customer_id
	,cus_segment
	,customer_industry
	,product_code
	,currency
	,disbursement_date
	,expiry_date
	,interest_rate
	,os_amount	
	,dpd
	,nhom_no_contract
	,ovd_prin_start_date
	,ovd_int_start_date
	,outstanding_avg_month
	,EXR_AT_DISBURSEMENT
from EWS..IFRS9_CTR_LC
where [ACCOUNT_STATUS] in ('A','H') or  [ACCOUNT_STATUS]  = 'S' and eomonth(EXPIRY_DATE) = PROCESS_DATE
)
union
select process_date
	,mud_code
	,BRANCH_CODE
	,contract_id
	,customer_id
	,cus_segment
	,customer_industry
	,product_code
	,currency
	,disbursement_date
	,expiry_date
	,interest_rate
	,os_amount	
	,dpd
	,nhom_no_contract
	,ovd_prin_start_date
	,ovd_int_start_date
	,outstanding_avg_month
	,EXR_AT_DISBURSEMENT
from EWS..IFRS9_CTR_OD
-- where SUBSTRING(convert(varchar,[GL_CODE]), 1, 1) in ('2', '3')
)
select distinct process_date
	,mud_code
	,BRANCH_CODE
	,contract_id
	,customer_id
	,cus_segment
	,customer_industry
	,product_code
	,currency
	,disbursement_date
	,expiry_date
	,interest_rate
	,os_amount	
	,dpd
	,nhom_no_contract
	,ovd_prin_start_date
	,ovd_int_start_date
	,outstanding_avg_month
	,EXR_AT_DISBURSEMENT
into ##EWS_CTR_GL23
from tbl
 where PROCESS_DATE between eomonth(DATEADD(month, -5, @run_date)) and @run_date
 and NOT (
( PRODUCT_CODE in ('ODIP','ODIF','NOIV','NOIU', 'NOIP', 'B680', 'OBS1', 'OBS2','L12S','L11M','L11S','L12M','L10M','L10S')
    OR LEFT(CONTRACT_ID,3) = '555'
    OR [BRANCH_CODE] = '555')  and mud_code = 'CL'
OR (mud_code <> 'CL' and BRANCH_CODE ='555')
OR (mud_code = 'LC' and PRODUCT_CODE in('B400','ELC1')
))
;
-- where PROCESS_DATE <= @run_date; -- for run back date


----------------------------------------------------------------------------------------------
-- chuan bi du lieu ews thang 10
-- note: phan nay chi ap dung khi lay du lieu thang 10, lay du lieu cac thang truoc co the khong chinh xac
--       do su dung ham lag
-- chay 1 lan khi khoi tao du lieu

with ctr
as (
	select distinct customer_id
	from ##EWS_CTR_GL23
	where [process_date] = @run_date
	)
,dd as(
select eomonth(DATEADD(month, 0, @run_date)) process_date
union 
select eomonth(DATEADD(month, -1, @run_date)) process_date
union 
select eomonth(DATEADD(month, -2, @run_date)) process_date
union 
select eomonth(DATEADD(month, -3, @run_date)) process_date
union 
select eomonth(DATEADD(month, -4, @run_date)) process_date
union 
select eomonth(DATEADD(month, -5, @run_date)) process_date
),
y as(
select * 
from dd, ctr
)
merge ##ews_shortlist x
using y
	on (x.process_date = y.process_date and x.customer_id = y.customer_id)
when not matched
	then
		insert (
			process_date
			,customer_id
			)
		values (
			y.process_date
			,y.customer_id
			);

----------------------------------------------------------------------------------------------

with y
as (
	select PROCESS_DATE = eomonth([PROCESS_DATE])
		,CONTRACT_ID = CUST_AC_NO
		,CUSTOMER_ID = CIF
		,TOD_END_DATE
		,VALUE_DATE
		,GLCODE
		,NHOM_NO_CONTRACT = NHOM_NO
		,dpd_od = IIF(PROCESS_DATE > TOD_END_DATE, DATEDIFF(DAY, TOD_END_DATE, PROCESS_DATE), 0)
	from [DATA]..[MIS035B]
	where RIGHT(GLCODE, 4) = '2000'
	and eomonth([PROCESS_DATE]) between eomonth(dateadd(month, - 5, @run_date))
			and @run_date
	)	
update x
set x.DPD = y.dpd_od
from ##EWS_CTR_GL23 x 
inner join y 
on (x.process_date = y.process_date and x.contract_id = y.contract_id and x.MUD_CODE = 'OD');

----------------------------------------------------------------------------------------------
-- update dpd_l3m, dpd_l6m

with a
as (
	select process_date
		,customer_id
		,max(case 
				when dpd > 0
					then dpd
				else 0
				end) dpd_final
	from ##EWS_CTR_GL23
	group by process_date
		,customer_id
	)
,y
as (
	select b.process_date
		,b.customer_id
		,case 
			when row_number() over (
					partition by b.customer_id order by b.process_date asc
					) >= 3
				then max(dpd_final) over (
						partition by b.customer_id order by b.process_date asc rows between 2 preceding
								and current row
						)
			end as dpd_l3m
		,case 
			when row_number() over (
					partition by b.customer_id order by b.process_date asc
					) >= 6
				then max(dpd_final) over (
						partition by b.customer_id order by b.process_date asc rows between 5 preceding
								and current row
						)
			end as dpd_l6m
	from a
	right join ##ews_shortlist b on (a.process_date = b.process_date and a.customer_id = b.customer_id)
	)
update x
set x.dpd_l3m = y.dpd_l3m
	,x.dpd_l6m = y.dpd_l6m
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
option (use hint('enable_parallel_plan_preference'));
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'dpd_l3m, dpd_l6m';
-- update avg_total_num_txn_l6m -----------------------------------------------------
with y
as (
	select b.process_date
		,b.customer_id
		,case 
			when [num_non_cash_txn] <> 0
				then [ebank_txn_amt] / [num_non_cash_txn]
			else null
			end as avg_non_cash_txn
		,case 
			when a.total_num_txn <> 0
				then avg_txn_amt_l1m / a.total_num_txn
			end as avg_total_num_txn
		,case 
			when row_number() over (
					partition by b.customer_id order by b.process_date asc
					) >= 3
				then avg(case 
							when a.total_num_txn <> 0
								then abs(a.avg_txn_amt_l1m) / a.total_num_txn
							else 0
							end) over (
						partition by b.customer_id order by b.process_date asc rows between 2 preceding
								and current row
						)
			end as avg_total_num_txn_l3m
		,case 
			when row_number() over (
					partition by b.customer_id order by b.process_date asc
					) >= 6
				then avg(case 
							when a.total_num_txn <> 0
								then abs(a.avg_txn_amt_l1m) / a.total_num_txn
							else 0
							end) over (
						partition by b.customer_id order by b.process_date asc rows between 5 preceding
								and current row
						)
			end as avg_total_num_txn_l6m
		,a.total_num_txn_l3m
		,a.max_net_flow_txn_amt_l3m
		,a.total_num_txn
	from [ews].[dbo].[ifrs9_dep_amt_txn] a
	right join ##ews_shortlist b on (a.process_date = b.process_date and a.customer_id = b.customer_id)
	)
update x
set x.avg_total_num_txn = y.avg_total_num_txn
	,x.total_num_txn_l3m = y.total_num_txn_l3m
	,x.max_net_flow_txn_amt_l3m = y.max_net_flow_txn_amt_l3m
	,x.total_num_txn = y.total_num_txn
	,x.avg_total_num_txn_l3m = y.avg_total_num_txn_l3m
	,x.avg_total_num_txn_l6m = y.avg_total_num_txn_l6m
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
option (use hint('enable_parallel_plan_preference'));
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'avg_total_num_txn';
-- update avg_cr_txn_amt_vs_os_l6m -----------------------------------------------------
with a
as (
	select bb.customer_id
		,bb.process_date
		,cr_txn_amt_l6m = case 
			when row_number() over (
					partition by bb.customer_id order by bb.process_date asc
					) >= 6
				then sum(isnull(cr_txn_amt, 0)) over (
						partition by bb.customer_id order by bb.process_date asc rows between 5 preceding
								and current row
						)
			end
		,avg_txn_amt_l3m
	from ews..ifrs9_dep_amt_txn aa
	right join ##ews_shortlist bb on (aa.process_date = bb.process_date and aa.customer_id = bb.customer_id)
	)
	,b
as (
	select bb.customer_id
		,bb.process_date
		,os_l6m = case 
			when row_number() over (
					partition by bb.customer_id order by bb.process_date asc
					) >= 6
				then sum(isnull(os, 0)) over (
						partition by bb.customer_id order by bb.process_date asc rows between 5 preceding
								and current row
						)
			end
		,os_onbs_l3m = case 
			when row_number() over (
					partition by bb.customer_id order by bb.process_date asc
					) >= 3
				then sum(isnull(OS_ONBS, 0)) over (
						partition by bb.customer_id order by bb.process_date asc rows between 2 preceding
								and current row
						)
			end
	from EWS..IFRS9_CST_LN aa
	right join ##ews_shortlist bb on (aa.process_date = bb.process_date and aa.customer_id = bb.customer_id)
	)
	,y
as (
	select a.customer_id
		,a.process_date
		,avg_cr_txn_amt_vs_os_l6m = case 
			when b.os_l6m <> 0
				then a.cr_txn_amt_l6m / b.os_l6m
			end
		,avg_txn_amt_os_onbs_l3m = case 
			when b.os_onbs_l3m <> 0
				then a.avg_txn_amt_l3m / b.os_onbs_l3m
			end
	from a
	inner join b on (a.process_date = b.process_date and a.customer_id = b.customer_id)
	)
update x
set x.avg_cr_txn_amt_vs_os_l6m = y.avg_cr_txn_amt_vs_os_l6m
	,x.avg_txn_amt_os_onbs_l3m = y.avg_txn_amt_os_onbs_l3m
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
option (use hint('enable_parallel_plan_preference'));
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'avg_cr_txn_amt_vs_os_l6m';
-- update max_dpd_loan ------------------------------------------------------

with y
as (
	select process_date
		,customer_id
		,max(case 
				when dpd > 0
					then dpd
				else 0
				end) max_dpd_loan
	from ##EWS_CTR_GL23
	where [MUD_CODE] = 'CL'
	group by process_date
		,customer_id
	)
update x
set x.max_dpd_loan = y.max_dpd_loan
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'max_dpd_loan';
--- ews..ifrs9_dep_amt -----------------------------------------------------------------------
with y
as (
	select customer_id
		,process_date
		,min_ca_bal_l6m
		,total_bal_avg
		,avg_bal_l6m
		,bal_l6m
		,ca_bal_l3m = case 
			when row_number() over (
					partition by customer_id order by [process_date] asc
					) >= 3
				then avg(isnull(ca_bal, 0)) over (
						partition by customer_id order by [process_date] asc rows between 2 preceding
								and current row
						)
			end -- ca_bal_l3m truoc lay so tu mart
		,ca_bal
		,ca_td_bal_legacy
		,avg_bal_l3m = case 
			when row_number() over (
					partition by customer_id order by [process_date] asc
					) >= 3
				then avg(isnull(total_bal_avg, 0)) over (
						partition by customer_id order by [process_date] asc rows between 2 preceding
								and current row
						)
			end
	from ews..ifrs9_dep_amt
	)
update x
set x.min_ca_bal_l6m = y.min_ca_bal_l6m
	,x.total_bal_avg = y.total_bal_avg
	,x.avg_bal_l3m = y.avg_bal_l3m
	,x.avg_bal_l6m = y.avg_bal_l6m
	,x.bal_l6m = y.bal_l6m
	,x.ca_bal_l3m = y.ca_bal_l3m
	,x.ca_bal = y.ca_bal
	,x.ca_td_bal_legacy = y.ca_td_bal_legacy
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ifrs9_dep_amt';
--- ews..ifrs9_cst_ln -----------------------------------------------------------------------
with y
as (
	select bb.customer_id
		,bb.process_date
		,MAX_OS_L6M = case 
			when row_number() over (
					partition by bb.customer_id order by bb.process_date asc
					) >= 6
				then max(isnull(os, 0)) over (
						partition by bb.customer_id order by bb.process_date asc rows between 5 preceding
								and current row
						)
			end
		,MIN_OS_L6M = case 
			when row_number() over (
					partition by bb.customer_id order by bb.process_date asc
					) >= 6
				then min(isnull(os, 0)) over (
						partition by bb.customer_id order by bb.process_date asc rows between 5 preceding
								and current row
						)
			end
		,max_os_onbs_l6m = case 
			when row_number() over (
					partition by bb.customer_id order by bb.process_date asc
					) >= 6
				then max(isnull(os_onbs, 0)) over (
						partition by bb.customer_id order by bb.process_date asc rows between 5 preceding
								and current row
						)
			end
	from EWS..IFRS9_CST_LN aa
	right join ##ews_shortlist bb on (aa.process_date = bb.process_date and aa.customer_id = bb.customer_id)
	)
update x
set x.vol_os_l6m = case 
		when isnull(y.MAX_OS_L6M, 0) = 0
			then 0
		else (isnull(y.MAX_OS_L6M, 0) - isnull(y.MIN_OS_L6M, 0)) / isnull(y.MAX_OS_L6M, 0)
		end
	,x.max_os_onbs_l6m = y.max_os_onbs_l6m
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ifrs9_cst_ln';
--- max_os_onbs_l6m, os_revl, num_cr_product_l3m -----------------------------------------------------------------------
update x
set x.os_revl = y.os_revl
	,x.num_cr_product_l3m = y.num_cr_product_l3m
from ##ews_shortlist as x
inner join ews..ifrs9_cst_ln y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'max_os_onbs_l6m, os_revl, num_cr_product_l3m';
--- avg_bal_onbs_l6m -----------------------------------------------------------------------


with bb as (
select process_date
      ,customer_id
	  ,avg_os_onbs = sum(outstanding_avg_month * EXR_AT_DISBURSEMENT)      
  from ##EWS_CTR_GL23
  where mud_code in ('CC', 'OD')
		or (mud_code = 'CL' and os_amount > 0)
  group by process_date
      ,customer_id
),
a
as (
	select aa.process_date
		,aa.customer_id
		,bb.avg_os_onbs
		,total_bal_os_onbs_avg = case 
			when bb.avg_os_onbs <> 0
				then aa.total_bal_avg / bb.avg_os_onbs
			end
	from ews..ifrs9_dep_amt as aa
	inner join bb on (aa.[process_date] = bb.[process_date] and aa.[customer_id] = bb.[customer_id])
	)
	,y
as (
	select b.process_date
		,b.customer_id
		,a.total_bal_os_onbs_avg
		,a.avg_os_onbs
		,avg_bal_onbs_l6m = case 
			when row_number() over (
					partition by b.customer_id order by b.process_date asc
					) >= 6
				then avg(isnull(a.total_bal_os_onbs_avg, 0)) over (
						partition by b.customer_id order by b.process_date asc rows between 5 preceding
								and current row
						)
			end
	from a
	right join ##ews_shortlist b on (a.process_date = b.process_date and a.customer_id = b.customer_id)
	)
update x
set x.avg_bal_onbs_l6m = y.avg_bal_onbs_l6m
, x.total_bal_os_onbs_avg = y.total_bal_os_onbs_avg
, x.avg_os_onbs = y.avg_os_onbs
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id);
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'avg_bal_onbs_l6m; total_bal_os_onbs_avg';
--- ca_bal_os_onbs_avg -----------------------------------------------------------------------
with b as (
select process_date
      ,customer_id
	  ,avg_os_onbs = sum(outstanding_avg_month  * EXR_AT_DISBURSEMENT)      
  from ##EWS_CTR_GL23
    where mud_code in ('CC', 'OD')
		or (mud_code = 'CL' and os_amount > 0)
  group by process_date
      ,customer_id
),
y
as (
	select a.process_date
		,a.customer_id
		,ca_bal_os_onbs_avg = case 
			when avg_os_onbs != 0
				then (ca_bal_avg / avg_os_onbs)
			else null
			end
	from ews..[ifrs9_dep_amt] a
	inner join b on (a.process_date = b.process_date and a.customer_id = b.customer_id)
	)
update x
set x.ca_bal_os_onbs_avg = y.ca_bal_os_onbs_avg
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ca_bal_os_onbs_avg';
--- ca_bal_os_onbs_l3m_avg -----------------------------------------------------------------------
with y
as (
	select a.process_date
		,a.customer_id
		,ca_bal_os_onbs_l3m_avg = case 
			when row_number() over (
					partition by customer_id order by process_date asc
					) >= 3
				then avg(isnull(ca_bal_os_onbs_avg, 0)) over (
						partition by customer_id order by process_date asc rows between 2 preceding
								and current row
						)
			end
	from ##ews_shortlist a
	)
update x
set x.ca_bal_os_onbs_l3m_avg = y.ca_bal_os_onbs_l3m_avg
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ca_bal_os_onbs_l3m_avg';
--- total_num_txn_l6m -----------------------------------------------------------------------
with y
as (
	select b.process_date
		,b.customer_id
		,total_num_txn_l6m = case 
			when row_number() over (
					partition by b.customer_id order by b.process_date asc
					) >= 6
				then sum(isnull(a.total_num_txn, 0)) over (
						partition by b.customer_id order by b.process_date asc rows between 5 preceding
								and current row
						)
			end
	from ews..[ifrs9_dep_amt_txn] a
	right join ##ews_shortlist b on (a.process_date = b.process_date and a.customer_id = b.customer_id)
	)
update x
set x.total_num_txn_l6m = y.total_num_txn_l6m
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'total_num_txn_l6m';
--- total_num_txn_l6m -----------------------------------------------------------------------
with y
as (
	select b.process_date
		,b.customer_id
		,os_revl_l3m = case 
			when row_number() over (
					partition by b.customer_id order by b.process_date asc
					) >= 3
				then avg(isnull(b.os_revl, 0)) over (
						partition by b.customer_id order by b.process_date asc rows between 2 preceding
								and current row
						)
			end
	from ##ews_shortlist b
	)
update x
set x.os_revl_l3m = y.os_revl_l3m
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'os_revl_l3m';
--- remain_term, remain_term_autoloan -----------------------------------------------------------------------

with tbl
as (
	select a.process_date
		,a.customer_id
		,a.contract_id
		,a.expiry_date
		,a.mud_code
		,a.os_amount * EXR_AT_DISBURSEMENT as os_amount
		,mob = datediff(month,cast(disbursement_date as date), cast(process_date as date))
		,remain_term = case 
			when a.expiry_date > a.process_date
				then convert(float, (a.expiry_date - a.process_date)) / 30
			else 0
			end
		,remain_term_autoloan = case 
			when (a.expiry_date > a.process_date and b.deal_sub_type = 'LN_AUTO')
				then convert(float, (a.expiry_date - a.process_date)) / 30
			else 0
			end
		,term = case 
			when (mud_code = 'cl' and expiry_date > disbursement_date)
				then (convert(float, expiry_date) - convert(float, disbursement_date)) / 30
			else 0
			end
		,day_to_matdt = case 
			when (mud_code = 'cl' and a.expiry_date > a.process_date)
				then (convert(float, expiry_date) - convert(float, process_date))
			else NULL
			end
		,dpd = case 
			when a.dpd > 0
				then a.dpd
			else 0
			end
	from ##EWS_CTR_GL23 a
	left join [ews]..[PRODUCT_CODE_IFRS9] b 
	on (a.product_code = b.product_code and a.process_date BETWEEN b.eff_date and b.EXP_DATE ) 	
	)
	,y
as (
	select process_date
		,customer_id
		,max(remain_term) remain_term
		,max(remain_term_autoloan) remain_term_autoloan
		,max(case 
				when dpd > 0
					then dpd
				else 0
				end) max_dpd
		,max(mob) mob
		,os_amt_long_curr = sum(case 
				when term > 60
					then os_amount  
				else 0
				end)
		,min_day_to_matdt = min(day_to_matdt)
		,os_amount = sum(os_amount)
	from tbl
	group by process_date
		,customer_id
	)
update x
set x.remain_term = y.remain_term
	,x.remain_term_autoloan = y.remain_term_autoloan
	,x.os_amt_long_curr = y.os_amt_long_curr
	,x.min_day_to_matdt = y.min_day_to_matdt
	,x.mob = y.mob
	,x.max_dpd = y.max_dpd
	,x.os_amount = y.os_amount
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id);
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'remain_term, remain_term_autoloan, os_amt_long_curr, min_day_to_matdt,os_amount';
--- cg_segment, ifrs9_segment -----------------------------------------------------------------------
-- lay segment dieu chinh tu cif
-- uu tien segment trong file loan -> tiep theo customer_info
with y
as (
	select a.process_date
		,a.customer_id
		,a.BRANCH_CODE
		,c.customer_type
		,isnull(a.cus_segment, c.cg_segment) cg_segment
		,isnull(b.segment, isnull(ref1.segment, ref2.segment)) ifrs9_segment
	from ##EWS_CTR_GL23 a
	left join ews..segment_adjustment b on a.customer_id = b.cif
	left join ews..[ifrs9_cust_info] c on (a.process_date = c.process_date and a.customer_id = c.customer_id)
	left join ews..segment_corp ref1 on a.cus_segment = ref1.cg_segment
	left join ews..segment_corp ref2 on c.cg_segment = ref2.cg_segment
	)
update x
set x.cg_segment = y.cg_segment
	,x.ifrs9_segment = y.ifrs9_segment
	,x.customer_type = y.customer_type
	,x.BRANCH_CODE = y.BRANCH_CODE
from ##ews_shortlist as x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id);
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'cg_segment, ifrs9_segment';

----- update mob, max_dpd theo ODR_contract_level
--update x
--set x.mob = y.MAX_MOB
--	,x.max_dpd = y.MAX_DPD_CURRENT
--from ##ews_shortlist as x
--inner join  [IFRS9_PROVISIONAL].[dbo].[CUSTOMER_ODR_LRA] y 
--on (x.process_date = y.process_date and x.customer_id = y.customer_id);
--exec ews..[b_prc_log_event] 'pd'
--	,@@rowcount
--	,'mob, max_dpd';

----------------------------------------------------------------------------------------------
-- current default
update x
set x.current_default = y.MAX_CURRENT_GB
from ##ews_shortlist as x
inner join [IFRS9_PROVISIONAL].[dbo].[CUSTOMER_ODR_LRA] y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'current_default';
----------------------------------------------------------------------------------------------
-- tao bang ews_portfolio
-- Bang nay dung de chia danh muc CL 
drop table
if exists ##ews_portfolio;

with tbl as (
		select a.process_date
			,a.mud_code
			,a.customer_id
			-- ,a.product_code
			,product_modelling = isnull(b.product_modelling, 'Others')
			,deal_sub_type = isnull(b.deal_sub_type, 'LN_OTHERS') 
			,a.disbursement_date
			,a.dpd
			,a.os_amount * a.EXR_AT_DISBURSEMENT as os_amount
		from ##EWS_CTR_GL23 a
		-- inner join [ews]..[product_code_retail] b on a.product_code = b.product_code
		-- update 2023-09-21: join between product code
		left join [ews]..[PRODUCT_CODE_IFRS9] b 
		on (a.product_code = b.product_code and a.process_date BETWEEN b.eff_date and b.EXP_DATE ) 
		where lower(a.mud_code) = 'cl' and a.process_date = @run_date 		
		)
	select process_date
		,mud_code
		,customer_id
		,product_modelling
		,deal_sub_type
		,max(datediff(month,cast(disbursement_date as date), cast(process_date as date))) mob_portfolio
		,max(case 
				when dpd > 0
					then dpd
				else 0
				end) max_dpd_portfolio
		,sum(os_amount) os_amount_by_portfolio
	into ##ews_portfolio
	from tbl
	group by process_date
		,mud_code
		,customer_id
		,deal_sub_type
		,product_modelling;
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'product_modelling change to deal_sub_type';
----------------------------------------------------------------------------------------------
--- update mob, max_dpd_portfolio theo bang [CUSTOMER_ODR_LRA]
update x
set x.max_dpd_portfolio = y.MAX_DPD_CURRENT,
	x.mob_portfolio = y.MAX_MOB
from ##ews_portfolio as x
inner join [IFRS9_PROVISIONAL].[dbo].[CUSTOMER_ODR_LRA] y on (x.process_date = y.process_date and x.customer_id = y.customer_id and x.product_modelling = y.SEGMENT_ODR_LV1)
----------------------------------------------------------------------------------------------
-- Bang cuoi cung
-- Thong tin khach hang chia theo tung danh muc 
-- Tính current good / bad de loai luon KH mob theo cap do danh muc
drop table
if exists ##rating_portfolio_shortlist;

select x.[mud_code]
      ,x.[product_modelling]	  
	  ,x.mob_portfolio
	  ,x.max_dpd_portfolio
	  ,x.[os_amount_by_portfolio]
      ,y.[process_date]
      ,y.[customer_id]
      ,y.[customer_type]
      ,y.[cg_segment]
      ,y.[ifrs9_segment]
      ,y.[current_default]
      ,y.[mob]
      ,y.[max_dpd]
      ,y.[dpd_l6m]
      ,y.[dpd_l3m]
      ,y.[min_ca_bal_l6m]
      ,y.[avg_total_num_txn_l6m]
      ,y.[avg_total_num_txn]
      ,y.[avg_total_num_txn_l3m]
      ,y.[avg_cr_txn_amt_vs_os_l6m]
      ,y.[avg_txn_amt_os_onbs_l3m]
      ,y.[vol_os_l6m]
      ,y.[remain_term_autoloan]
      ,y.[max_os_onbs_l6m]
      ,y.[avg_bal_onbs_l6m]
      ,y.[remain_term]
      ,y.[total_bal_avg]
      ,y.[ca_bal_os_onbs_avg]
      ,y.[total_num_txn_l6m]
      ,y.[avg_bal_l3m]
      ,y.[avg_bal_l6m]
      ,y.[ca_bal_os_onbs_l3m_avg]
      ,y.[bal_l6m]
      ,y.[os_amt_long_curr]
      ,y.[total_num_txn_l3m]
      ,y.[max_net_flow_txn_amt_l3m]
      ,y.[ca_bal_l3m]
      ,y.[max_dpd_loan]
      ,y.[total_num_txn]
      ,y.[os_revl_l3m]
      ,y.[os_revl]
      ,y.[ca_bal]
      ,y.[min_day_to_matdt]
      ,y.[num_cr_product_l3m]
      ,y.[ca_td_bal_legacy]
	  ,y.branch_code	  
	  ,x.deal_sub_type
	  ,bucket = case when current_default = 1 then 'B5: DPD >90'
	when isnull(max_dpd_portfolio, 0) between 0 and 30 and mob_portfolio < 6 then 'B1: MOB <6 & DPD 0-30'
	 when isnull(max_dpd_portfolio, 0) between 0 and 30 and mob_portfolio >= 6 then 'B2: MOB >=6 & DPD 0-30' 
	  when isnull(max_dpd_portfolio, 0) between 31 and 60 then 'B3: DPD 31-60'
	  when isnull(max_dpd_portfolio, 0) between 61 and 90 then 'B4: DPD 61-90'
	 end
	into ##rating_portfolio_shortlist
	from ##ews_portfolio x
	inner join ##ews_shortlist y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
	;

------update bucket theo [CUSTOMER_ODR_LRA]

update x
set x.bucket = y.BUCKET,
	x.mob_portfolio = y.MAX_MOB
from ##rating_portfolio_shortlist as x
inner join [IFRS9_PROVISIONAL].[dbo].[CUSTOMER_ODR_LRA] y on 
(x.process_date = y.process_date and x.customer_id = y.customer_id and x.product_modelling = y.SEGMENT_ODR_LV1)
/*
	where process_date = @run_date and lower(ifrs9_segment) in (
			'khcn'
			,'retail'
			) /*and lower(product_modelling) in (
			'autoloan'
			,'consumer lending - secured'
			,'consumer lending - unsecured'
			,'mortgage'
			,'others'
			) */
			and deal_sub_type in (
			'LN_AUTO',
			'LN_CONS_SEC',
			'LN_CONS_UNSEC',
			'LN_MORTGAGE',
			'LN_OTHERS'
			)			
			and isnull(os_amount_by_portfolio, 0) > 0;
			*/

drop table
if exists ##ews_portfolio_shortlist;

select *,
model = case when current_default = 1 then 'Default'
			when deal_sub_type in (
						'LN_AUTO',
						'LN_CONS_SEC',
						'LN_CONS_UNSEC',
						'LN_MORTGAGE',
						'LN_OTHERS'
						) and mob_portfolio >= 6 
						and max_dpd_portfolio <= 30 						
						and isnull(current_default,0) = 0
			then 'B-score'
			else 'ODR LRA'
			end
	into ##ews_portfolio_shortlist -- bang cuoi cung de du bao ews
	from ##rating_portfolio_shortlist
	where process_date = @run_date and lower(ifrs9_segment) in (
			'khcn'
			,'retail'
			)
		and isnull(os_amount_by_portfolio, 0) > 0 ;

	/*
	where process_date = @run_date and lower(ifrs9_segment) in (
			'khcn'
			,'retail'
			) /*and lower(product_modelling) in (
			'autoloan'
			,'consumer lending - secured'
			,'consumer lending - unsecured'
			,'mortgage'
			,'others'
			) */
			and deal_sub_type in (
			'LN_AUTO',
			'LN_CONS_SEC',
			'LN_CONS_UNSEC',
			'LN_MORTGAGE',
			'LN_OTHERS'
			) and mob_portfolio >= 6 and max_dpd_portfolio <= 30 and isnull(os_amount_by_portfolio, 0) > 0 and current_default = 0 ;
	*/

----------------------------------------------------------------------------------------------
---- -- THEM SO LIEU KHDN
----------------------------------------------------------------------------------------------
with y
as (
	select customer_id
		,process_date
		,num_xdpd_l6m = case 
			when row_number() over (
					partition by customer_id order by process_date asc
					) >= 6
				then sum(case 
							when isnull(max_dpd, 0) > 0
								then 1
							else 0
							end) over (
						partition by customer_id order by process_date asc rows between 5 preceding
								and current row
						)
			end
	from ##ews_shortlist
	)
update x
set x.num_xdpd_l6m = y.num_xdpd_l6m
from y
inner join ##ews_shortlist x on (x.process_date = y.process_date and x.customer_id = y.customer_id);

exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'num_xdpd_l6m';
-- avg_ebank_txn_amt_l3m ----------------------------------------------------------------------
update x
set x.avg_ebank_txn_amt_l3m = y.avg_ebank_txn_amt_l3m
-- ,x.avg_cash_txn_amt_per_time_l3m = y.avg_cash_txn_amt_per_time_l3m
,x.avg_cash_txn_amt_per_time_l3m = case when y.NUM_NON_CASH_TXN <> 0 then y.CASH_TXN_AMT_L3M/y.NUM_NON_CASH_TXN end
from [ews]..[ifrs9_dep_amt_txn] y
inner join ##ews_shortlist x on (x.process_date = y.process_date and x.customer_id = y.customer_id);
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'avg_ebank_txn_amt_l3m, avg_cash_txn_amt_per_time_l3m';

-- Bang so lieu cuoi cung cho KHDN
drop table if exists ##rating_portfolio_shortlist_KHDN;
-- loai nhung KH CL co os_amount = 0
with y
as (
	select process_date
		,customer_id
		,sum(abs(os_amount  * EXR_AT_DISBURSEMENT)) os_amount
	from ##EWS_CTR_GL23
	where mud_code in (
			'CC'
			,'OD'
			) or (mud_code = 'CL' and os_amount > 0)
	group by process_date
		,customer_id
	)
select x.process_date
	,x.customer_id
	,min_ca_bal_l6m
	,vol_os_l6m
	,num_cr_product_l3m
	,max_dpd as dpd
	,num_xdpd_l6m
	,total_bal_os_onbs_avg
	,avg_ebank_txn_amt_l3m
	,avg_cash_txn_amt_per_time_l3m
	,ifrs9_segment
	,customer_type
	,mob
	,current_default
	,y.os_amount
	,branch_code
	,bucket = case when current_default = 1 then 'B5: DPD >90'
				when isnull(max_dpd, 0) between 0 and 30 and mob < 6 then 'B1: MOB <6 & DPD 0-30'
				when isnull(max_dpd, 0) between 0 and 30 and mob >= 6 then 'B2: MOB >=6 & DPD 0-30' 
				when isnull(max_dpd, 0) between 31 and 60 then 'B3: DPD 31-60'
				when isnull(max_dpd, 0) between 61 and 90 then 'B4: DPD 61-90'
				when current_default is null then null
				end
-- into ##ews_shortlist_khdn
into ##rating_portfolio_shortlist_KHDN
from ##ews_shortlist x
inner join y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
where -- customer_type = 'C'
	ifrs9_segment in (
		'Bigcorp 1'
		,'Bigcorp 2'
		,'Medium 1'
		,'Medium 2'
		,'Micro'
		,'Small'
		,'Upper 1'
		,'Upper 2'
		) and x.process_date = @run_date;
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'rating_portfolio_shortlist_KHDN';
-- avg_ebank_txn_amt_l3m ----------------------------------------------------------------------

-- update ngay 21/09/2023
update x
set x.bucket = y.BUCKET,
	x.mob = y.MAX_MOB,
	x.dpd = y.MAX_DPD_CURRENT,
	x.current_default  = y.MAX_CURRENT_GB
from ##rating_portfolio_shortlist_KHDN as x
inner join [IFRS9_PROVISIONAL].[dbo].[CUSTOMER_ODR_LRA] y 
on (x.process_date = y.process_date and x.customer_id = y.customer_id);

drop table if exists ##ews_shortlist_khdn;
select *,
model = case when current_default = 1 then 'Default'
			when mob >= 6 and dpd <= 30 and current_default = 0		
			then 'B-score'
			when current_default is null then null
			else 'ODR LRA'
			end
into ##ews_shortlist_khdn
from ##rating_portfolio_shortlist_KHDN x
where -- customer_type = 'C'
	ifrs9_segment in (
		'Bigcorp 1'
		,'Bigcorp 2'
		,'Medium 1'
		,'Medium 2'
		,'Micro'
		,'Small'
		,'Upper 1'
		,'Upper 2'
		) and x.process_date = @run_date;
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ews_shortlist_khdn';
------------------------------------------------------------------------
-- Xu ly missing, gia tri am
-- Tinh PD, score, grade
-- grade co the bi lech do cong thuc tinh woe
----------------------------------------------------------------------

drop table if exists ##woe_tbl;
	select *
		,case 
			when isnull(total_bal_os_onbs_avg, 0) <= 0.000371938609972325
				then - 0.938966346409836
			when isnull(total_bal_os_onbs_avg, 0) > 0.000371938609972325 and isnull(total_bal_os_onbs_avg, 0) <= 0.000799223284924705
				then - 0.911497056522708
			when isnull(total_bal_os_onbs_avg, 0) > 0.000799223284924705 and isnull(total_bal_os_onbs_avg, 0) <= 0.00148090191497434
				then - 0.223889443893686
			when isnull(total_bal_os_onbs_avg, 0) > 0.00148090191497434 and isnull(total_bal_os_onbs_avg, 0) <= 0.00328043557558119
				then 0.209757470275477
			when isnull(total_bal_os_onbs_avg, 0) > 0.00328043557558119 and isnull(total_bal_os_onbs_avg, 0) <= 0.00402075883799372
				then 0.530704408193358
			when isnull(total_bal_os_onbs_avg, 0) > 0.00402075883799372 and isnull(total_bal_os_onbs_avg, 0) <= 0.00636851767224609
				then 0.825762447336825
			when isnull(total_bal_os_onbs_avg, 0) > 0.00636851767224609 and isnull(total_bal_os_onbs_avg, 0) <= 0.0187660601268799
				then 0.84073202669684
			when isnull(total_bal_os_onbs_avg, 0) > 0.0187660601268799 and isnull(total_bal_os_onbs_avg, 0) <= 0.0898362716309556
				then 0.921577402593214
			when isnull(total_bal_os_onbs_avg, 0) > 0.0898362716309556
				then 1.3860461543119
			end total_bal_os_onbs_avg_woe
		,case 
			when isnull(min_ca_bal_l6m, 0) <= 38
				then - 1.39106143938033
			when isnull(min_ca_bal_l6m, 0) > 38
				then 0.238200688893887
			end min_ca_bal_l6m_woe
		,case 
			when isnull(vol_os_l6m, 0) <= 0.0689655230328232
				then - 0.893163507482064
			when isnull(vol_os_l6m, 0) > 0.0689655230328232 and isnull(vol_os_l6m, 0) <= 0.0847457592930691
				then - 0.363081833020957
			when isnull(vol_os_l6m, 0) > 0.0847457592930691 and isnull(vol_os_l6m, 0) <= 0.105633802221781
				then - 0.0828535967277566
			when isnull(vol_os_l6m, 0) > 0.105633802221781 and isnull(vol_os_l6m, 0) <= 0.552419703685469
				then 0.178038285364624
			when isnull(vol_os_l6m, 0) > 0.552419703685469
				then 0.458625727441491
			end vol_os_l6m_woe
		,case 
			when isnull(num_xdpd_l6m, 0) <= 0
				then 1.14328980664335
			when isnull(num_xdpd_l6m, 0) > 0 and isnull(num_xdpd_l6m, 0) <= 1
				then - 0.178401784567709
			when isnull(num_xdpd_l6m, 0) > 1 and isnull(num_xdpd_l6m, 0) <= 3
				then - 0.853982373013688
			when isnull(num_xdpd_l6m, 0) > 3
				then - 2.14485246561275
			end num_xdpd_l6m_woe
		,case 
			when isnull(dpd, 0) <= 1
				then 0.67322388724542
			when isnull(dpd, 0) > 1
				then - 1.83118935316134
			end dpd_woe
		,case 
			when isnull(avg_ebank_txn_amt_l3m, 0) <= 10000
				then 0.297859174209366
			when isnull(avg_ebank_txn_amt_l3m, 0) > 10000 and isnull(avg_ebank_txn_amt_l3m, 0) <= 6898333.33333333
				then - 0.159635129473812
			when isnull(avg_ebank_txn_amt_l3m, 0) > 6898333.33333333 and isnull(avg_ebank_txn_amt_l3m, 0) <= 12109333.3333333
				then - 0.4380678051287
			when isnull(avg_ebank_txn_amt_l3m, 0) > 12109333.3333333
				then - 0.617920764956258
			end avg_ebank_txn_amt_l3m_woe
		,case 
			when isnull(num_cr_product_l3m, 0) <= 1
				then 0.204522989810662
			when isnull(num_cr_product_l3m, 0) > 1
				then - 0.42776278745523
			end num_cr_product_l3m_woe
		,case 
			when isnull(avg_cash_txn_amt_per_time_l3m, 0) <= 55000
				then 0.190166371596229
			when isnull(avg_cash_txn_amt_per_time_l3m, 0) > 55000 and isnull(avg_cash_txn_amt_per_time_l3m, 0) <= 11833333.3333333
				then 0.0643436597545541
			when isnull(avg_cash_txn_amt_per_time_l3m, 0) > 11833333.3333333 and isnull(avg_cash_txn_amt_per_time_l3m, 0) <= 16660000
				then - 0.143731122600736
			when isnull(avg_cash_txn_amt_per_time_l3m, 0) > 16660000 and isnull(avg_cash_txn_amt_per_time_l3m, 0) <= 26400000
				then - 0.28928874711718
			when isnull(avg_cash_txn_amt_per_time_l3m, 0) > 26400000
				then - 0.29756920360242
			end avg_cash_txn_amt_per_time_l3m_woe
	into ##woe_tbl
	from ##ews_shortlist_khdn
;

drop table if exists ##pd_tbl;

with 
logitp_tbl as(
select *
	,logitp = - 3.876128433429840 
	- 0.657847614407099 * num_xdpd_l6m_woe 
	- 0.330242192223948 * dpd_woe 
	- 0.394566326046101 * total_bal_os_onbs_avg_woe 
	- 0.538916897750402 * min_ca_bal_l6m_woe 
	- 0.694141094246267 * avg_ebank_txn_amt_l3m_woe  
	- 0.513064257204425 * vol_os_l6m_woe  
	- 0.450545112420598 * num_cr_product_l3m_woe  
	- 1.974420696105690 * avg_cash_txn_amt_per_time_l3m_woe 
from ##woe_tbl
)
select *, prob_bad = exp(logitp)/(1 + exp(logitp))
		 , score = 1000*(1 - exp(logitp)/(1 + exp(logitp)))	
		 into ##pd_tbl
		 from logitp_tbl

-- 2023-09-21: update bucket theo bang CUSTOMER_ODR_LRA 

drop table if exists ##map_grade_khdn;
select *
	,grade = 
	case when bucket = 'B5: DPD >90' then 'Default'
		when bucket = 'B1: MOB <6 & DPD 0-30' then 'Unrated'
		when bucket = 'B3: DPD 31-60' then 'D1'
		when bucket = 'B4: DPD 61-90' then 'D2'		
		when score >= 997.7087658 and score < 1000
			then 'AAA'
		when score >= 997.0471743 and score < 997.7087658
			then 'AA'
		when score >= 995.3866 and score < 997.0471743
			then 'A'
		when score >= 993.22820805 and score < 995.3866
			then 'BBB'
		when score >= 988.3248326 and score < 993.22820805
			then 'BB'
		when score >= 976.7325986 and score < 988.3248326
			then 'B'
		when score >= 935.0005576 and score < 976.7325986
			then 'CCC'
		when score >= 830.7454558 and score < 935.0005576
			then 'CC'
		when score >= 635.6692527 and score < 830.7454558
			then 'C'
		when score >= 0 and score < 635.6692527
			then 'D'		
		end 
		into ##map_grade_khdn
		from ##pd_tbl
		;


drop table if exists ##ews_khdn_score;
select a.*, b.ttc_pd, b.grade_num
into ##ews_khdn_score
from ##map_grade_khdn a
-- ngay 2023-09-22: thay doi bang map thanh master_scale_portfolio
-- left join ews..master_scale_khdn b
left join EWS.dbo.master_scale_portfolio b
on (a.grade = b.grade and a.process_date between b.eff_date and b.exp_date and b.portfolio = 'Non-retail')
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ews_khdn_score';

-- update grade_num = 13 where grade = 'Default'
update x
set x.grade_num = 13
from ##ews_khdn_score x
where x.grade = 'Default'
-- update model = 'ODR LRA' where grade = 'unrated'
update x
set x.model = 'ODR LRA'
from ##ews_khdn_score x
where x.grade = 'unrated'

/*
select *
	,case 
		when score >= 997.7087658 and score < 1000
			then 'AAA'
		when score >= 997.0471743 and score < 997.7087658
			then 'AA'
		when score >= 995.3866 and score < 997.0471743
			then 'A'
		when score >= 993.22820805 and score < 995.3866
			then 'BBB'
		when score >= 988.3248326 and score < 993.22820805
			then 'BB'
		when score >= 976.7325986 and score < 988.3248326
			then 'B'
		when score >= 935.0005576 and score < 976.7325986
			then 'CCC'
		when score >= 830.7454558 and score < 935.0005576
			then 'CC'
		when score >= 635.6692527 and score < 830.7454558
			then 'C'
		when score >= 0 and score < 635.6692527
			then 'D'
		end grade
	,case 
		when score >= 997.7087658 and score < 1000
			then 0.0007162738
		when score >= 997.0471743 and score < 997.7087658
			then 0.0021136813
		when score >= 995.3866 and score < 997.0471743
			then 0.0041236661
		when score >= 993.22820805 and score < 995.3866
			then 0.0080450263
		when score >= 988.3248326 and score < 993.22820805
			then 0.0156953658
		when score >= 976.7325986 and score < 988.3248326
			then 0.0306207214
		when score >= 935.0005576 and score < 976.7325986
			then 0.0597391988
		when score >= 830.7454558 and score < 935.0005576
			then 0.1165476093
		when score >= 635.6692527 and score < 830.7454558
			then 0.2273774258
		when score >= 0 and score < 635.6692527
			then 0.4435997793
		end ttc_pd
	,case 
		when score >= 997.7087658 and score < 1000
			then 1
		when score >= 997.0471743 and score < 997.7087658
			then 2
		when score >= 995.3866 and score < 997.0471743
			then 3
		when score >= 993.22820805 and score < 995.3866
			then 4
		when score >= 988.3248326 and score < 993.22820805
			then 5
		when score >= 976.7325986 and score < 988.3248326
			then 6
		when score >= 935.0005576 and score < 976.7325986
			then 7
		when score >= 830.7454558 and score < 935.0005576
			then 8
		when score >= 635.6692527 and score < 830.7454558
			then 9
		when score >= 0 and score < 635.6692527
			then 10
		end grade_num
into ##ews_khdn_score
from pd_tbl;

*/



------------------------------------------------------------------------------------------------
-- Dung de check ly do loai khach hang
drop table
if exists ews..ews_total;


with tbl as(
select x.[mud_code]
      ,x.deal_sub_type
	  ,x.mob_portfolio
	  ,x.max_dpd_portfolio
	  ,x.[os_amount_by_portfolio]
      ,y.[process_date]
      ,y.[customer_id]
      ,y.[customer_type]
      ,y.[cg_segment]
      ,y.[ifrs9_segment]
      ,y.[current_default]
      ,y.[mob]
      ,y.[max_dpd]
      ,y.[dpd_l6m]
      ,y.[dpd_l3m]
      ,y.[min_ca_bal_l6m]
      ,y.[avg_total_num_txn_l6m]
      ,y.[avg_total_num_txn]
      ,y.[avg_total_num_txn_l3m]
      ,y.[avg_cr_txn_amt_vs_os_l6m]
      ,y.[avg_txn_amt_os_onbs_l3m]
      ,y.[vol_os_l6m]
      ,y.[remain_term_autoloan]
      ,y.[max_os_onbs_l6m]
      ,y.[avg_bal_onbs_l6m]
      ,y.[remain_term]
      ,y.[total_bal_avg]
      ,y.[ca_bal_os_onbs_avg]
      ,y.[total_num_txn_l6m]
      ,y.[avg_bal_l3m]
      ,y.[avg_bal_l6m]
      ,y.[ca_bal_os_onbs_l3m_avg]
      ,y.[bal_l6m]
      ,y.[os_amt_long_curr]
      ,y.[total_num_txn_l3m]
      ,y.[max_net_flow_txn_amt_l3m]
      ,y.[ca_bal_l3m]
      ,y.[max_dpd_loan]
      ,y.[total_num_txn]
      ,y.[os_revl_l3m]
      ,y.[os_revl]
      ,y.[ca_bal]
      ,y.[min_day_to_matdt]
      ,y.[num_cr_product_l3m]
      ,y.[ca_td_bal_legacy]
	  ,y.branch_code	  
	from ##ews_portfolio x
	inner join ##ews_shortlist y on (x.process_date = y.process_date and x.customer_id = y.customer_id)
	), rating as (
	select *
	,bucket = case when current_default = 1 then 'B5: DPD >90'
	when max_dpd_portfolio between 0 and 30 and mob_portfolio < 6 then 'B1: MOB <6 & DPD 0-30'
	 when max_dpd_portfolio between 0 and 30 and mob_portfolio >= 6 then 'B2: MOB >=6 & DPD 0-30' 
	  when max_dpd_portfolio between 31 and 60 then 'B3: DPD 31-60'
	  when max_dpd_portfolio between 61 and 90 then 'B4: DPD 61-90'
	  end
	from tbl
	)
	select *
	,segment_lv1 = case when lower(ifrs9_segment) in (
			'khcn'
			,'retail'
			) then deal_sub_type
			when ifrs9_segment in (
		'Bigcorp 1'
		,'Bigcorp 2'
		,'Medium 1'
		,'Medium 2'
		,'Micro'
		,'Small'
		,'Upper 1'
		,'Upper 2'
		) then 'NON-RETAIL'
		else 'UNKNOWN'
		end 
	,criteria_filter = case when lower(ifrs9_segment) in (
			'khcn'
			,'retail'
			)/*and lower(product_modelling) in (
			'autoloan'
			,'consumer lending - secured'
			,'consumer lending - unsecured'
			,'mortgage'
			,'others'
			) */
			and deal_sub_type in (
			'LN_AUTO',
			'LN_CONS_SEC',
			'LN_CONS_UNSEC',
			'LN_MORTGAGE',
			'LN_OTHERS'
			)
			and isnull(os_amount_by_portfolio, 0) > 0 
			and mob_portfolio >= 6 
			and max_dpd_portfolio <= 30 
			and current_default = 0 then 'pass'
			when lower(ifrs9_segment) not in (
			'khcn'
			,'retail'
			) then 'not khcn'
			when /*and lower(product_modelling) in (
			'autoloan'
			,'consumer lending - secured'
			,'consumer lending - unsecured'
			,'mortgage'
			,'others'
			) */
			 deal_sub_type in (
			'LN_AUTO',
			'LN_CONS_SEC',
			'LN_CONS_UNSEC',
			'LN_MORTGAGE',
			'LN_OTHERS'
			) then 'not product loan'
			when isnull(os_amount_by_portfolio, 0) <= 0 then 'not os_amount_by_portfolio > 0'
			when mob_portfolio < 6 then 'not mob_portfolio >= 6'
			when max_dpd_portfolio > 30 then 'not max_dpd_portfolio <= 30'
			when current_default <> 0 then 'not current_default = 0'
			else 'not' end
	into ews..ews_total
	from rating 
	where process_date = @run_date ;
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ews_total';
-----------------------------------------------------------------------------------------------------------
-- Luu du lieu hang thang
-----------------------------------------------------------------------------------------------------------
-- KHCN
delete from EWS..ews_portfolio_shortlist_store  
where process_date = @run_date ;

insert into EWS..ews_portfolio_shortlist_store  
select [mud_code]
      ,product_modelling
      ,[process_date]
      ,[customer_id]
      ,[customer_type]
      ,[cg_segment]
      ,[ifrs9_segment]
      ,[current_default]
      ,[mob]
      ,[max_dpd]
      ,[dpd_l6m]
      ,[dpd_l3m]
      ,[min_ca_bal_l6m]
      ,[avg_total_num_txn_l6m]
      ,[avg_total_num_txn]
      ,[avg_total_num_txn_l3m]
      ,[avg_cr_txn_amt_vs_os_l6m]
      ,[avg_txn_amt_os_onbs_l3m]
      ,[vol_os_l6m]
      ,[remain_term_autoloan]
      ,[max_os_onbs_l6m]
      ,[avg_bal_onbs_l6m]
      ,[remain_term]
      ,[total_bal_avg]
      ,[ca_bal_os_onbs_avg]
      ,[total_num_txn_l6m]
      ,[avg_bal_l3m]
      ,[avg_bal_l6m]
      ,[ca_bal_os_onbs_l3m_avg]
      ,[bal_l6m]
      ,[os_amt_long_curr]
      ,[total_num_txn_l3m]
      ,[max_net_flow_txn_amt_l3m]
      ,[ca_bal_l3m]
      ,[max_dpd_loan]
      ,[total_num_txn]
      ,[os_revl_l3m]
      ,[os_revl]
      ,[ca_bal]
      ,[min_day_to_matdt]
      ,[num_cr_product_l3m]
      ,[ca_td_bal_legacy]
      ,[mob_portfolio]
      ,[max_dpd_portfolio] 
	  ,[os_amount_by_portfolio]
	  ,branch_code
	  ,deal_sub_type
	  ,bucket
	  ,model
	  from ##ews_portfolio_shortlist
	  where process_date = @run_date ;
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ews_portfolio_shortlist';
-----------------------------------------------------------------------------------------------------------
-- Luu du lieu KHDN
-----------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Tinh diem
-- Xu ly missing, so am
-- Chuyen doi woe
-- tinh pd va diem
------------------------------------------------------------------------------------------------
delete from ews..ews_khdn_score_store
where process_date = @run_date ;

insert into ews..ews_khdn_score_store
select 
[process_date]
      ,[customer_id]
      ,[min_ca_bal_l6m]
      ,[vol_os_l6m]
      ,[num_cr_product_l3m]
      ,[dpd]
      ,[num_xdpd_l6m]
      ,[total_bal_os_onbs_avg]
      ,[avg_ebank_txn_amt_l3m]
      ,[avg_cash_txn_amt_per_time_l3m]
      ,[ifrs9_segment]
      ,[customer_type]
      ,[mob]
      ,[current_default]
      ,[os_amount]
      ,[branch_code]
      ,[total_bal_os_onbs_avg_woe]
      ,[min_ca_bal_l6m_woe]
      ,[vol_os_l6m_woe]
      ,[num_xdpd_l6m_woe]
      ,[dpd_woe]
      ,[avg_ebank_txn_amt_l3m_woe]
      ,[num_cr_product_l3m_woe]
      ,[avg_cash_txn_amt_per_time_l3m_woe]
      ,[logitp]
      ,[prob_bad]
      ,[score]
      ,[grade]
      ,[ttc_pd]
      ,[grade_num]
	  ,[bucket]
	  ,[model]
	  from ##ews_khdn_score;
exec ews..[b_prc_log_event] 'pd'
	,@@rowcount
	,'ews_khdn_score';
