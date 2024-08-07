USE [EWS]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC  [dbo].[rule_mapping_CTR_CL_CL020]
AS
----- map CL020 với CUST_INFO lấy CG_Segment
DECLARE	@run_date date = '2024-02-29';

delete from EWS.[dbo].[IFRS9_CTR_CL_CL020]
where PROCESS_DATE = @run_date;

drop table if exists #CL020;
SELECT distinct t1.PROCESS_DATE
      ,t1.CONTRACT_ID
      ,REPLACE(REPLACE
	  ((SELECT CAST(NULLIF(LIMIT_ID,'') AS VARCHAR(20)) + '/' 
	  FROM DATA..CL020 t2 
	  WHERE t2.CONTRACT_ID = t1.CONTRACT_ID 
	  AND t2.PROCESS_DATE = t1.PROCESS_DATE 
	  FOR XML PATH('')), '', ','), '/,', '/') AS LIMIT_ID
into #CL020
FROM DATA..CL020 t1
WHERE PROCESS_DATE = @run_date
GROUP BY PROCESS_DATE,CONTRACT_ID

---------------------------------------------------
-------------------------------------------------------------
insert into EWS.[dbo].[IFRS9_CTR_CL_CL020](
process_date, contract_id, LIMIT_ID)
select process_date, contract_id, LIMIT_ID
from #CL020
where PROCESS_DATE = @run_date


--13236806
--13236171
update x								
set x.MUD_CODE ='CL'
	,x.CUSTOMER_ID = y.CUSTOMER_ID
	,x.CURRENCY = y.CCY
	,x.BRANCH_CODE =  CONCAT(REPLICATE(0,3-LEN(y.BRANCH_CODE)),y.BRANCH_CODE)
	,x.ACCOUNT_STATUS = y.STATUS
	,x.PRODUCT_GROUP = 'On_B'
	,x.DISBURSEMENT_DATE = y.DISBURSEMENT_DATE
	,x.EXPIRY_DATE = y.EXPIRY_DATE
	,x.PRINCIPAL_OVERDUE_AMOUNT = y.OD_PRINCIPLE
	,x.INTEREST_OVERDUE_AMOUNT = y.OD_INTEREST
	,x.NHOM_NO_CONTRACT=y.NHOM_NO_CONTRACT
	,x.OVD_PRIN_START_DATE= y.FIRST_PRIN_OVD_DATE
	,x.OVD_INT_START_DATE= y.FIRST_INT_OVD_DATE
	,x.DISBURSEMENT_AMOUNT=y.DISBURSEMENT_AMOUNT	
	,x.EXR_AT_DISBURSEMENT=y.EXCHANGE_RATE
	,x.OS_AMOUNT=y.OS_AMOUNT
from EWS.[dbo].[IFRS9_CTR_CL_CL020] x 								
inner join DATA..CL020 y 								
on (x.process_date = y.process_date and x.contract_id = y.contract_id)
where x.PROCESS_DATE = @run_date

---------Update DPD------------------------

	drop table if exists #tbl;
	select process_date							
			,customer_id						
			,contract_id						
			,od_principle						
			,first_prin_ovd_date						
			,od_interest						
			,first_int_ovd_date						
			,snqh_goc = case 						
				when od_principle > 0					
					then datediff(day, first_prin_ovd_date, process_date)				
				end					
			,snqh_lai = case 						
				when od_interest > 0					
					then datediff(day, first_int_ovd_date, process_date)				
				end	
	into #tbl
	from DATA..CL020	
	where PROCESS_DATE = @run_date;
	-------------------------------
	drop table if exists #y;
	select process_date
			,CONTRACT_ID
			,dpd_cl = max(case 						
				when isnull(snqh_goc, 0) > isnull(snqh_lai, 0)					
					then snqh_goc				
				else snqh_lai					
				end)	
	into #y
	from #tbl	
	group by process_date
			,CONTRACT_ID
	-------------------------------------						
	update x								
	set x.DPD = #y.dpd_cl								
	from EWS.[dbo].[IFRS9_CTR_CL_CL020] x 								
	inner join #y 								
	on (x.process_date = #y.process_date and x.contract_id = #y.contract_id);


with tbl as (
select *
, ROW_NUMBER() OVER (PARTITION BY contract_id, process_date ORDER BY PRINCIPAL_PAYMENT_FREQUENCY) AS RowRank from [dbo].[IFRS9_CTR_CL]
where process_date = @run_date
)
select * 
into #tmp_ctr_cl
from tbl
where RowRank = 1;

---------------------------------------
update x								
set x.RPT_DT_DIM_ID=y.RPT_DT_DIM_ID
	,x.PRODUCT_CODE = y.PRODUCT_CODE	
	,x.CUS_SEGMENT = y.CUS_SEGMENT
	,x.CUSTOMER_INDUSTRY=y.CUSTOMER_INDUSTRY
	,x.ALTERNATIVE_ACCOUNT_NUMBER=y.ALTERNATIVE_ACCOUNT_NUMBER
	,x.GL_CODE = y.GL_CODE
	,x.INTEREST_RATE = y.INTEREST_RATE
	,x.OUTSTANDING_AVG_MONTH = y.OUTSTANDING_AVG_MONTH
	,x.PRINCIPAL_OS_AMOUNT = y.PRINCIPAL_OS_AMOUNT
	,x.INTEREST_OS_AMOUNT=y.INTEREST_OS_AMOUNT
	,x.INTEREST_PENALTY_AMOUNT=y.INTEREST_PENALTY_AMOUNT
	,x.PRINCIPAL_PAYMENT_FREQUENCY = y.PRINCIPAL_PAYMENT_FREQUENCY
	,x.INTEREST_PAYMENT_FREQUENCY = y.INTEREST_PAYMENT_FREQUENCY
	,x.NEXT_PRINCIPAL_PAYMENT_AMOUNT = y.NEXT_PRINCIPAL_PAYMENT_AMOUNT
	,x.NEXT_INTEREST_PAYMENT_AMOUNT = y.NEXT_INTEREST_PAYMENT_AMOUNT
	,x.NEXT_PRINCIPAL_PAYMENT_DATE = y.NEXT_PRINCIPAL_PAYMENT_DATE
	,x.NEXT_INTEREST_PAYMENT_DATE = y.NEXT_INTEREST_PAYMENT_DATE
	,x.MAX_DPD=y.MAX_DPD
	,x.UTILIZATION_RATE=y.UTILIZATION_RATE
	,x.NGAY_HET_AN_HAN_GOC=y.NGAY_HET_AN_HAN_GOC
	,x.LTV=y.LTV
	,x.LTV_DISBURSEMENT=y.LTV_DISBURSEMENT
	,x.WRITE_OFF_TAG=y.WRITE_OFF_TAG
	,x.WRITE_OFF_DATE=y.WRITE_OFF_DATE
	,x.WRITE_OFF_AMOUNT=y.WRITE_OFF_AMOUNT
	,x.LOAN_PURPOSE_MDV=y.LOAN_PURPOSE_MDV
	,x.LOAN_PURPOSE_UDF=y.LOAN_PURPOSE_UDF
	,x.LOAN_PURPOSE_INTERNAL=y.LOAN_PURPOSE_INTERNAL
	,x.OS_SCH_NEXT_MONTH=y.OS_SCH_NEXT_MONTH
	,x.OS_SCH_THIS_MONTH=y.OS_SCH_THIS_MONTH
	,x.PREPAYMENT_AMOUNT=y.PREPAYMENT_AMOUNT
	,x.EARLY_REDEMPTION_FLAG=y.EARLY_REDEMPTION_FLAG
	,x.OVD_YN_STATUS=y.OVD_YN_STATUS
	,x.OVD_30D_YN_STATUS=y.OVD_30D_YN_STATUS
	,x.AR_NBR=y.AR_NBR
	,x.ACCOUNT_STATUS = y.ACCOUNT_STATUS
	,x.LIMIT_AMT = y.LIMIT_AMT
from EWS.[dbo].[IFRS9_CTR_CL_CL020] x 								
inner join #tmp_ctr_cl y 								
on (x.process_date = eomonth(y.process_date) and x.contract_id = y.contract_id)
where x.PROCESS_DATE = @run_date


SELECT DISTINCT CUSTOMER_ID, CG_SEGMENT 
into #tmp_cg_segment
from ews.dbo.IFRS9_CUST_INFO
WHERE CG_SEGMENT is not null
and PROCESS_DATE = @run_date;

update x								
set x.CUS_SEGMENT = y.CG_SEGMENT
from EWS.[dbo].[IFRS9_CTR_CL_CL020] x 								
inner join #tmp_cg_segment y 								
on (x.CUSTOMER_ID = y.CUSTOMER_ID)
where x.PROCESS_DATE = @run_date;


update EWS.[dbo].[IFRS9_CTR_CL_CL020] 
set LIMIT_ID = LEFT(LIMIT_ID, LEN(LIMIT_ID)- 1)
where right(LIMIT_ID,1)='/'
and PROCESS_DATE = @run_date;
GO


