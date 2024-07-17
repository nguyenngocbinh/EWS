-- Step 1: Tao bang temp co TCTD 
drop table if exists rb_ctnv;
create temporary table rb_ctnv as
(with t1 as
(select *, (select MID_RATE 
				from CSO.FCC_CYTB_RATES_HISTORY_OFFICIAL 
				where CCY1 = 'USD' 
				and RATE_DATE <= cast(ctnv.RPT_DT as date)
				order by RATE_DATE desc fetch first 1 rows only) as MID_RATE 
	from CSO.CIC_RB06_CTNV ctnv)
, t2 as
(select distinct 
	RPT_DT
	, MSPHIEU
	, MA_CIC
	, NGAY_BAOCAO
	, MA_TCTD
	, TEN_TCTD
	, LOAIVAY
	, NHOMNO
	, DUNO_VND
	, DUNO_USD
	, DUNO_USD * MID_RATE as DUNO_USD_QUY_DOI
	, TONG_VND
	, TONG_USD
	, TONG_USD * MID_RATE as TONG_USD_QUY_DOI
	, case when length(MA_TCTD) = 3 then MA_TCTD else SUBSTRING(ma_tctd,3,3) end as TCTD
	from t1)
select *, TONG_VND + TONG_USD_QUY_DOI as TONG_DU_NO 
	from t2);
	
-- Tao bien theo ma_tctd level
drop table if exists bank_level;
create temporary table bank_level as
(select ma_cic, rpt_dt, ma_tctd
, sum(case when loaivay = '01' 	then TONG_DU_NO else 0 end) as LOAN_OS_SHORT_TERM
, sum(case when loaivay = '02' 	then TONG_DU_NO else 0 end) as LOAN_OS_MEDIUM_TERM
, sum(case when loaivay = '03' 	then TONG_DU_NO else 0 end) as LOAN_OS_LONG_TERM
, sum(case when loaivay = '13' 	then TONG_DU_NO else 0 end) as LOAN_OS_OTHER
, sum(case when loaivay = '23' 	then TONG_DU_NO else 0 end) as LOAN_OS_OVERDUE
, sum(TONG_DU_NO) as LOAN_OS_TOTAL
, sum(case when nhomno = '01' 					then TONG_DU_NO else 0 end) as LOAN_OS_OD1
, sum(case when nhomno = '02' 					then TONG_DU_NO else 0 end) as LOAN_OS_OD2
, sum(case when nhomno in ('01', '02') 			then TONG_DU_NO else 0 end) as LOAN_OS_OD12
, sum(case when nhomno in ('03', '04', '05') 	then TONG_DU_NO else 0 end) as LOAN_OS_OD345
, sum(case when nhomno = '01' 					then 1 else 0 end) as LOAN_CNT_OD1
, sum(case when nhomno = '02' 					then 1 else 0 end) as LOAN_CNT_OD2
, sum(case when nhomno in ('01', '02') 			then 1 else 0 end) as LOAN_CNT_OD12
, sum(case when nhomno in ('03', '04', '05') 	then 1 else 0 end) as LOAN_CNT_OD345
, sum(case when loaivay = '01' and nhomno = '01' 				then TONG_DU_NO else 0 end) as LOAN_OS_SHORT_OD1
, sum(case when loaivay = '01' and nhomno = '02' 				then TONG_DU_NO else 0 end) as LOAN_OS_SHORT_OD2
, sum(case when loaivay = '01' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end) as LOAN_OS_SHORT_OD12
, sum(case when loaivay = '01' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end) as LOAN_OS_SHORT_OD345
, sum(case when loaivay = '02' and nhomno = '01' 				then TONG_DU_NO else 0 end) as LOAN_OS_MEDIUM_OD1
, sum(case when loaivay = '02' and nhomno = '02' 				then TONG_DU_NO else 0 end) as LOAN_OS_MEDIUM_OD2
, sum(case when loaivay = '02' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end) as LOAN_OS_MEDIUM_OD12
, sum(case when loaivay = '02' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end) as LOAN_OS_MEDIUM_OD345
, sum(case when loaivay = '03' and nhomno = '01' 				then TONG_DU_NO else 0 end) as LOAN_OS_LONG_OD1
, sum(case when loaivay = '03' and nhomno = '02' 				then TONG_DU_NO else 0 end) as LOAN_OS_LONG_OD2
, sum(case when loaivay = '03' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end) as LOAN_OS_LONG_OD12
, sum(case when loaivay = '03' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end) as LOAN_OS_LONG_OD345
, sum(case when loaivay = '13' and nhomno = '01' 				then TONG_DU_NO else 0 end) as LOAN_OS_OTHER_OD1
, sum(case when loaivay = '13' and nhomno = '02' 				then TONG_DU_NO else 0 end) as LOAN_OS_OTHER_OD2
, sum(case when loaivay = '13' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end) as LOAN_OS_OTHER_OD12
, sum(case when loaivay = '13' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end) as LOAN_OS_OTHER_OD345
	from rb_ctnv
	group by ma_cic, rpt_dt, ma_tctd);

-- Tao bien phai sinh min, max, avg, sum
select ma_cic, rpt_dt
, sum(LOAN_OS_SHORT_TERM) as LOAN_OS_SHORT_TERM_SUM
, min(LOAN_OS_SHORT_TERM) as LOAN_OS_SHORT_TERM_MIN
, max(LOAN_OS_SHORT_TERM) as LOAN_OS_SHORT_TERM_MAX
, avg(LOAN_OS_SHORT_TERM) as LOAN_OS_SHORT_TERM_AVG
, sum(LOAN_OS_MEDIUM_TERM) as LOAN_OS_MEDIUM_TERM_SUM
, min(LOAN_OS_MEDIUM_TERM) as LOAN_OS_MEDIUM_TERM_MIN
, max(LOAN_OS_MEDIUM_TERM) as LOAN_OS_MEDIUM_TERM_MAX
, avg(LOAN_OS_MEDIUM_TERM) as LOAN_OS_MEDIUM_TERM_AVG
, sum(LOAN_OS_LONG_TERM) as LOAN_OS_LONG_TERM_SUM
, min(LOAN_OS_LONG_TERM) as LOAN_OS_LONG_TERM_MIN
, max(LOAN_OS_LONG_TERM) as LOAN_OS_LONG_TERM_MAX
, avg(LOAN_OS_LONG_TERM) as LOAN_OS_LONG_TERM_AVG
, sum(LOAN_OS_OTHER) as LOAN_OS_OTHER_SUM
, min(LOAN_OS_OTHER) as LOAN_OS_OTHER_MIN
, max(LOAN_OS_OTHER) as LOAN_OS_OTHER_MAX
, avg(LOAN_OS_OTHER) as LOAN_OS_OTHER_AVG
, sum(LOAN_OS_OVERDUE) as LOAN_OS_OVERDUE_SUM
, min(LOAN_OS_OVERDUE) as LOAN_OS_OVERDUE_MIN
, max(LOAN_OS_OVERDUE) as LOAN_OS_OVERDUE_MAX
, avg(LOAN_OS_OVERDUE) as LOAN_OS_OVERDUE_AVG
, sum(LOAN_OS_TOTAL) as LOAN_OS_TOTAL_SUM
, min(LOAN_OS_TOTAL) as LOAN_OS_TOTAL_MIN
, max(LOAN_OS_TOTAL) as LOAN_OS_TOTAL_MAX
, avg(LOAN_OS_TOTAL) as LOAN_OS_TOTAL_AVG
, sum(LOAN_OS_OD1) as LOAN_OS_OD1_SUM
, min(LOAN_OS_OD1) as LOAN_OS_OD1_MIN
, max(LOAN_OS_OD1) as LOAN_OS_OD1_MAX
, avg(LOAN_OS_OD1) as LOAN_OS_OD1_AVG
, sum(LOAN_OS_OD2) as LOAN_OS_OD2_SUM
, min(LOAN_OS_OD2) as LOAN_OS_OD2_MIN
, max(LOAN_OS_OD2) as LOAN_OS_OD2_MAX
, avg(LOAN_OS_OD2) as LOAN_OS_OD2_AVG
, sum(LOAN_OS_OD12) as LOAN_OS_OD12_SUM
, min(LOAN_OS_OD12) as LOAN_OS_OD12_MIN
, max(LOAN_OS_OD12) as LOAN_OS_OD12_MAX
, avg(LOAN_OS_OD12) as LOAN_OS_OD12_AVG
, sum(LOAN_OS_OD345) as LOAN_OS_OD345_SUM
, min(LOAN_OS_OD345) as LOAN_OS_OD345_MIN
, max(LOAN_OS_OD345) as LOAN_OS_OD345_MAX
, avg(LOAN_OS_OD345) as LOAN_OS_OD345_AVG
, sum(LOAN_CNT_OD1) as LOAN_CNT_OD1_SUM
, min(LOAN_CNT_OD1) as LOAN_CNT_OD1_MIN
, max(LOAN_CNT_OD1) as LOAN_CNT_OD1_MAX
, avg(LOAN_CNT_OD1) as LOAN_CNT_OD1_AVG
, sum(LOAN_CNT_OD2) as LOAN_CNT_OD2_SUM
, min(LOAN_CNT_OD2) as LOAN_CNT_OD2_MIN
, max(LOAN_CNT_OD2) as LOAN_CNT_OD2_MAX
, avg(LOAN_CNT_OD2) as LOAN_CNT_OD2_AVG
, sum(LOAN_CNT_OD12) as LOAN_CNT_OD12_SUM
, min(LOAN_CNT_OD12) as LOAN_CNT_OD12_MIN
, max(LOAN_CNT_OD12) as LOAN_CNT_OD12_MAX
, avg(LOAN_CNT_OD12) as LOAN_CNT_OD12_AVG
, sum(LOAN_CNT_OD345) as LOAN_CNT_OD345_SUM
, min(LOAN_CNT_OD345) as LOAN_CNT_OD345_MIN
, max(LOAN_CNT_OD345) as LOAN_CNT_OD345_MAX
, avg(LOAN_CNT_OD345) as LOAN_CNT_OD345_AVG
, sum(LOAN_OS_SHORT_OD1) as LOAN_OS_SHORT_OD1_SUM
, min(LOAN_OS_SHORT_OD1) as LOAN_OS_SHORT_OD1_MIN
, max(LOAN_OS_SHORT_OD1) as LOAN_OS_SHORT_OD1_MAX
, avg(LOAN_OS_SHORT_OD1) as LOAN_OS_SHORT_OD1_AVG
, sum(LOAN_OS_SHORT_OD2) as LOAN_OS_SHORT_OD2_SUM
, min(LOAN_OS_SHORT_OD2) as LOAN_OS_SHORT_OD2_MIN
, max(LOAN_OS_SHORT_OD2) as LOAN_OS_SHORT_OD2_MAX
, avg(LOAN_OS_SHORT_OD2) as LOAN_OS_SHORT_OD2_AVG
, sum(LOAN_OS_SHORT_OD12) as LOAN_OS_SHORT_OD12_SUM
, min(LOAN_OS_SHORT_OD12) as LOAN_OS_SHORT_OD12_MIN
, max(LOAN_OS_SHORT_OD12) as LOAN_OS_SHORT_OD12_MAX
, avg(LOAN_OS_SHORT_OD12) as LOAN_OS_SHORT_OD12_AVG
, sum(LOAN_OS_SHORT_OD345) as LOAN_OS_SHORT_OD345_SUM
, min(LOAN_OS_SHORT_OD345) as LOAN_OS_SHORT_OD345_MIN
, max(LOAN_OS_SHORT_OD345) as LOAN_OS_SHORT_OD345_MAX
, avg(LOAN_OS_SHORT_OD345) as LOAN_OS_SHORT_OD345_AVG
, sum(LOAN_OS_MEDIUM_OD1) as LOAN_OS_MEDIUM_OD1_SUM
, min(LOAN_OS_MEDIUM_OD1) as LOAN_OS_MEDIUM_OD1_MIN
, max(LOAN_OS_MEDIUM_OD1) as LOAN_OS_MEDIUM_OD1_MAX
, avg(LOAN_OS_MEDIUM_OD1) as LOAN_OS_MEDIUM_OD1_AVG
, sum(LOAN_OS_MEDIUM_OD2) as LOAN_OS_MEDIUM_OD2_SUM
, min(LOAN_OS_MEDIUM_OD2) as LOAN_OS_MEDIUM_OD2_MIN
, max(LOAN_OS_MEDIUM_OD2) as LOAN_OS_MEDIUM_OD2_MAX
, avg(LOAN_OS_MEDIUM_OD2) as LOAN_OS_MEDIUM_OD2_AVG
, sum(LOAN_OS_MEDIUM_OD12) as LOAN_OS_MEDIUM_OD12_SUM
, min(LOAN_OS_MEDIUM_OD12) as LOAN_OS_MEDIUM_OD12_MIN
, max(LOAN_OS_MEDIUM_OD12) as LOAN_OS_MEDIUM_OD12_MAX
, avg(LOAN_OS_MEDIUM_OD12) as LOAN_OS_MEDIUM_OD12_AVG
, sum(LOAN_OS_MEDIUM_OD345) as LOAN_OS_MEDIUM_OD345_SUM
, min(LOAN_OS_MEDIUM_OD345) as LOAN_OS_MEDIUM_OD345_MIN
, max(LOAN_OS_MEDIUM_OD345) as LOAN_OS_MEDIUM_OD345_MAX
, avg(LOAN_OS_MEDIUM_OD345) as LOAN_OS_MEDIUM_OD345_AVG
, sum(LOAN_OS_LONG_OD1) as LOAN_OS_LONG_OD1_SUM
, min(LOAN_OS_LONG_OD1) as LOAN_OS_LONG_OD1_MIN
, max(LOAN_OS_LONG_OD1) as LOAN_OS_LONG_OD1_MAX
, avg(LOAN_OS_LONG_OD1) as LOAN_OS_LONG_OD1_AVG
, sum(LOAN_OS_LONG_OD2) as LOAN_OS_LONG_OD2_SUM
, min(LOAN_OS_LONG_OD2) as LOAN_OS_LONG_OD2_MIN
, max(LOAN_OS_LONG_OD2) as LOAN_OS_LONG_OD2_MAX
, avg(LOAN_OS_LONG_OD2) as LOAN_OS_LONG_OD2_AVG
, sum(LOAN_OS_LONG_OD12) as LOAN_OS_LONG_OD12_SUM
, min(LOAN_OS_LONG_OD12) as LOAN_OS_LONG_OD12_MIN
, max(LOAN_OS_LONG_OD12) as LOAN_OS_LONG_OD12_MAX
, avg(LOAN_OS_LONG_OD12) as LOAN_OS_LONG_OD12_AVG
, sum(LOAN_OS_LONG_OD345) as LOAN_OS_LONG_OD345_SUM
, min(LOAN_OS_LONG_OD345) as LOAN_OS_LONG_OD345_MIN
, max(LOAN_OS_LONG_OD345) as LOAN_OS_LONG_OD345_MAX
, avg(LOAN_OS_LONG_OD345) as LOAN_OS_LONG_OD345_AVG
, sum(LOAN_OS_OTHER_OD1) as LOAN_OS_OTHER_OD1_SUM
, min(LOAN_OS_OTHER_OD1) as LOAN_OS_OTHER_OD1_MIN
, max(LOAN_OS_OTHER_OD1) as LOAN_OS_OTHER_OD1_MAX
, avg(LOAN_OS_OTHER_OD1) as LOAN_OS_OTHER_OD1_AVG
, sum(LOAN_OS_OTHER_OD2) as LOAN_OS_OTHER_OD2_SUM
, min(LOAN_OS_OTHER_OD2) as LOAN_OS_OTHER_OD2_MIN
, max(LOAN_OS_OTHER_OD2) as LOAN_OS_OTHER_OD2_MAX
, avg(LOAN_OS_OTHER_OD2) as LOAN_OS_OTHER_OD2_AVG
, sum(LOAN_OS_OTHER_OD12) as LOAN_OS_OTHER_OD12_SUM
, min(LOAN_OS_OTHER_OD12) as LOAN_OS_OTHER_OD12_MIN
, max(LOAN_OS_OTHER_OD12) as LOAN_OS_OTHER_OD12_MAX
, avg(LOAN_OS_OTHER_OD12) as LOAN_OS_OTHER_OD12_AVG
, sum(LOAN_OS_OTHER_OD345) as LOAN_OS_OTHER_OD345_SUM
, min(LOAN_OS_OTHER_OD345) as LOAN_OS_OTHER_OD345_MIN
, max(LOAN_OS_OTHER_OD345) as LOAN_OS_OTHER_OD345_MAX
, avg(LOAN_OS_OTHER_OD345) as LOAN_OS_OTHER_OD345_AVG
from bank_level
group by ma_cic, rpt_dt;

-- Tao bien theo msphieu level
select ma_cic, rpt_dt
, sum(case when loaivay = '01' 	then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_SHORT_TERM_RATIO
, sum(case when loaivay = '02' 	then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_TERM
, sum(case when loaivay = '03' 	then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_TERM_RATIO
, sum(case when loaivay = '13' 	then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_RATIO
, sum(case when loaivay = '23' 	then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OVERDUE_RATIO
, sum(case when loaivay = '01' 	then 1 else 0 end) as LOAN_CT_SHORT_TERM
, sum(case when loaivay = '02' 	then 1 else 0 end) as LOAN_CT_MEDIUM_TERM
, sum(case when loaivay = '03' 	then 1 else 0 end) as LOAN_CT_OTHER_TERM
, sum(case when loaivay = '13' 	then 1 else 0 end) as LOAN_CT_OTHER
, sum(case when loaivay = '23' 	then 1 else 0 end) as LOAN_CT_OVERDUE
, count(1) as LOAN_CT_TOTAL
, sum(case when loaivay = '01' 	then 1 else 0 end)/count(1) as LOAN_CT_SHORT_TERM_RATIO
, sum(case when loaivay = '02' 	then 1 else 0 end)/count(1) as LOAN_CT_MEDIUM_TERM_RATIO
, sum(case when loaivay = '03' 	then 1 else 0 end)/count(1) as LOAN_CT_OTHER_TERM_RATIO
, sum(case when loaivay = '13' 	then 1 else 0 end)/count(1) as LOAN_CT_OTHER_RATIO
, sum(case when loaivay = '23' 	then 1 else 0 end)/count(1) as LOAN_CT_OVERDUE_RATIO
, sum(case when nhomno = '01' 					then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OD1_RATIO
, sum(case when nhomno = '02' 					then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OD2_RATIO
, sum(case when nhomno in ('01', '02') 			then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OD12_RATIO
, sum(case when nhomno in ('03', '04', '05') 	then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OD345_RATIO
, sum(case when nhomno = '01' 					then 1 else 0 end)/count(1) as LOAN_CNT_OD1_RATIO
, sum(case when nhomno = '02' 					then 1 else 0 end)/count(1) as LOAN_CNT_OD2_RATIO
, sum(case when nhomno in ('01', '02') 			then 1 else 0 end)/count(1) as LOAN_CNT_OD12_RATIO
, sum(case when nhomno in ('03', '04', '05') 	then 1 else 0 end)/count(1) as LOAN_CNT_OD345_RATIO
, sum(case when loaivay = '01' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_SHORT_OD1_TOTAL_RATIO
, sum(case when loaivay = '01' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_SHORT_OD2_TOTAL_RATIO
, sum(case when loaivay = '01' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_SHORT_OD12_TOTAL_RATIO
, sum(case when loaivay = '01' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_SHORT_OD345_TOTAL_RATIO
, sum(case when loaivay = '01' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '01' then TONG_DU_NO else null end) as LOAN_OS_SHORT_OD1_RATIO
, sum(case when loaivay = '01' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '01' then TONG_DU_NO else null end) as LOAN_OS_SHORT_OD2_RATIO
, sum(case when loaivay = '01' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(case when loaivay = '01' then TONG_DU_NO else null end) as LOAN_OS_SHORT_OD12_RATIO
, sum(case when loaivay = '01' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(case when loaivay = '01' then TONG_DU_NO else null end) as LOAN_OS_SHORT_OD345_RATIO
, sum(case when loaivay = '02' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD1_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD2_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD12_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD345_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD1_RATIO
, sum(case when loaivay = '02' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD2_RATIO
, sum(case when loaivay = '02' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD12_RATIO
, sum(case when loaivay = '02' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD345_RATIO
, sum(case when loaivay = '02' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD1_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD2_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD12_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_MEDIUM_OD345_TOTAL_RATIO
, sum(case when loaivay = '02' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD1_RATIO
, sum(case when loaivay = '02' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD2_RATIO
, sum(case when loaivay = '02' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD12_RATIO
, sum(case when loaivay = '02' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(case when loaivay = '02' then TONG_DU_NO else null end) as LOAN_OS_MEDIUM_OD345_RATIO
, sum(case when loaivay = '03' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD1_TOTAL_RATIO
, sum(case when loaivay = '03' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD2_TOTAL_RATIO
, sum(case when loaivay = '03' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD12_TOTAL_RATIO
, sum(case when loaivay = '03' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD345_TOTAL_RATIO
, sum(case when loaivay = '03' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD1_RATIO
, sum(case when loaivay = '03' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD2_RATIO
, sum(case when loaivay = '03' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD12_RATIO
, sum(case when loaivay = '03' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD345_RATIO
, sum(case when loaivay = '13' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD1_TOTAL_RATIO
, sum(case when loaivay = '13' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD2_TOTAL_RATIO
, sum(case when loaivay = '13' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD12_TOTAL_RATIO
, sum(case when loaivay = '13' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(TONG_DU_NO) as LOAN_OS_OTHER_OD345_TOTAL_RATIO
, sum(case when loaivay = '13' and nhomno = '01' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD1_RATIO
, sum(case when loaivay = '13' and nhomno = '02' 				then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD2_RATIO
, sum(case when loaivay = '13' and nhomno in ('01', '02') 		then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD12_RATIO
, sum(case when loaivay = '13' and nhomno in ('03', '04', '05') then TONG_DU_NO else 0 end)/sum(case when loaivay = '03' then TONG_DU_NO else null end) as LOAN_OS_OTHER_OD345_RATIO
	from rb_ctnv
	group by ma_cic, rpt_dt
	
