USE [EWS]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Drop the existing procedure if it exists
IF OBJECT_ID('[dbo].[rule_mapping_CTR_CL_CL020]', 'P') IS NOT NULL
    DROP PROCEDURE [dbo].[rule_mapping_CTR_CL_CL020]
GO

CREATE PROC [dbo].[rule_mapping_CTR_CL_CL020]
@run_date DATE = NULL
AS
BEGIN
    -- Set default run date if not provided
    IF @run_date IS NULL
        SET @run_date = EOMONTH(DATEADD(MONTH, -1, GETDATE()));

    -- Delete existing data for the run date
    DELETE FROM EWS.[dbo].[IFRS9_CTR_CL_CL020]
    WHERE PROCESS_DATE = @run_date;

    -- Drop temporary table if it exists
    DROP TABLE IF EXISTS #CL020;

    -- Create temporary table with distinct contract IDs and their LIMIT_IDs
    SELECT DISTINCT t1.PROCESS_DATE,
                    t1.CONTRACT_ID,
                    REPLACE(REPLACE(
                        (SELECT CAST(NULLIF(LIMIT_ID,'') AS VARCHAR(20)) + '/' 
                        FROM DATA..CL020 t2 
                        WHERE t2.CONTRACT_ID = t1.CONTRACT_ID 
                        AND t2.PROCESS_DATE = t1.PROCESS_DATE 
                        FOR XML PATH('')), '', ','), '/,', '/') AS LIMIT_ID
    INTO #CL020
    FROM DATA..CL020 t1
    WHERE PROCESS_DATE = @run_date
    GROUP BY PROCESS_DATE, CONTRACT_ID;

    -- Insert data into the target table
    INSERT INTO EWS.[dbo].[IFRS9_CTR_CL_CL020] (process_date, contract_id, LIMIT_ID)
    SELECT process_date, contract_id, LIMIT_ID
    FROM #CL020
    WHERE PROCESS_DATE = @run_date;

    -- Update fields from source table
    UPDATE x
    SET x.MUD_CODE ='CL',
        x.CUSTOMER_ID = y.CUSTOMER_ID,
        x.CURRENCY = y.CCY,
        x.BRANCH_CODE = CONCAT(REPLICATE(0,3-LEN(y.BRANCH_CODE)),y.BRANCH_CODE),
        x.ACCOUNT_STATUS = y.STATUS,
        x.PRODUCT_GROUP = 'On_B',
        x.DISBURSEMENT_DATE = y.DISBURSEMENT_DATE,
        x.EXPIRY_DATE = y.EXPIRY_DATE,
        x.PRINCIPAL_OVERDUE_AMOUNT = y.OD_PRINCIPLE,
        x.INTEREST_OVERDUE_AMOUNT = y.OD_INTEREST,
        x.NHOM_NO_CONTRACT = y.NHOM_NO_CONTRACT,
        x.OVD_PRIN_START_DATE = y.FIRST_PRIN_OVD_DATE,
        x.OVD_INT_START_DATE = y.FIRST_INT_OVD_DATE,
        x.DISBURSEMENT_AMOUNT = y.DISBURSEMENT_AMOUNT,
        x.EXR_AT_DISBURSEMENT = y.EXCHANGE_RATE,
        x.OS_AMOUNT = y.OS_AMOUNT
    FROM EWS.[dbo].[IFRS9_CTR_CL_CL020] x
    INNER JOIN DATA..CL020 y
    ON (x.process_date = y.process_date AND x.contract_id = y.contract_id)
    WHERE x.PROCESS_DATE = @run_date;

    -- Update DPD values
    DROP TABLE IF EXISTS #tbl;
    SELECT process_date,
           customer_id,
           contract_id,
           od_principle,
           first_prin_ovd_date,
           od_interest,
           first_int_ovd_date,
           snqh_goc = CASE 
                       WHEN od_principle > 0 THEN DATEDIFF(day, first_prin_ovd_date, process_date)
                      END,
           snqh_lai = CASE 
                       WHEN od_interest > 0 THEN DATEDIFF(day, first_int_ovd_date, process_date)
                      END
    INTO #tbl
    FROM DATA..CL020
    WHERE PROCESS_DATE = @run_date;

    DROP TABLE IF EXISTS #y;
    SELECT process_date,
           CONTRACT_ID,
           dpd_cl = MAX(CASE 
                         WHEN ISNULL(snqh_goc, 0) > ISNULL(snqh_lai, 0) THEN snqh_goc
                         ELSE snqh_lai
                       END)
    INTO #y
    FROM #tbl
    GROUP BY process_date, CONTRACT_ID;

    UPDATE x
    SET x.DPD = #y.dpd_cl
    FROM EWS.[dbo].[IFRS9_CTR_CL_CL020] x
    INNER JOIN #y
    ON (x.process_date = #y.process_date AND x.contract_id = #y.contract_id);

    -- Handling rows with rank = 1
    WITH tbl AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY contract_id, process_date ORDER BY PRINCIPAL_PAYMENT_FREQUENCY) AS RowRank
        FROM [dbo].[IFRS9_CTR_CL]
        WHERE process_date = @run_date
    )
    SELECT *
    INTO #tmp_ctr_cl
    FROM tbl
    WHERE RowRank = 1;

    -- Update various fields from temp table
    UPDATE x
    SET x.RPT_DT_DIM_ID = y.RPT_DT_DIM_ID,
        x.PRODUCT_CODE = y.PRODUCT_CODE,
        x.CUS_SEGMENT = y.CUS_SEGMENT,
        x.CUSTOMER_INDUSTRY = y.CUSTOMER_INDUSTRY,
        x.ALTERNATIVE_ACCOUNT_NUMBER = y.ALTERNATIVE_ACCOUNT_NUMBER,
        x.GL_CODE = y.GL_CODE,
        x.INTEREST_RATE = y.INTEREST_RATE,
        x.OUTSTANDING_AVG_MONTH = y.OUTSTANDING_AVG_MONTH,
        x.PRINCIPAL_OS_AMOUNT = y.PRINCIPAL_OS_AMOUNT,
        x.INTEREST_OS_AMOUNT = y.INTEREST_OS_AMOUNT,
        x.INTEREST_PENALTY_AMOUNT = y.INTEREST_PENALTY_AMOUNT,
        x.PRINCIPAL_PAYMENT_FREQUENCY = y.PRINCIPAL_PAYMENT_FREQUENCY,
        x.INTEREST_PAYMENT_FREQUENCY = y.INTEREST_PAYMENT_FREQUENCY,
        x.NEXT_PRINCIPAL_PAYMENT_AMOUNT = y.NEXT_PRINCIPAL_PAYMENT_AMOUNT,
        x.NEXT_INTEREST_PAYMENT_AMOUNT = y.NEXT_INTEREST_PAYMENT_AMOUNT,
        x.NEXT_PRINCIPAL_PAYMENT_DATE = y.NEXT_PRINCIPAL_PAYMENT_DATE,
        x.NEXT_INTEREST_PAYMENT_DATE = y.NEXT_INTEREST_PAYMENT_DATE,
        x.MAX_DPD = y.MAX_DPD,
        x.UTILIZATION_RATE = y.UTILIZATION_RATE,
        x.NGAY_HET_AN_HAN_GOC = y.NGAY_HET_AN_HAN_GOC,
        x.LTV = y.LTV,
        x.LTV_DISBURSEMENT = y.LTV_DISBURSEMENT,
        x.WRITE_OFF_TAG = y.WRITE_OFF_TAG,
        x.WRITE_OFF_DATE = y.WRITE_OFF_DATE,
        x.WRITE_OFF_AMOUNT = y.WRITE_OFF_AMOUNT,
        x.LOAN_PURPOSE_MDV = y.LOAN_PURPOSE_MDV,
        x.LOAN_PURPOSE_UDF = y.LOAN_PURPOSE_UDF,
        x.LOAN_PURPOSE_INTERNAL = y.LOAN_PURPOSE_INTERNAL,
        x.OS_SCH_NEXT_MONTH = y.OS_SCH_NEXT_MONTH,
        x.OS_SCH_THIS_MONTH = y.OS_SCH_THIS_MONTH,
        x.PREPAYMENT_AMOUNT = y.PREPAYMENT_AMOUNT,
        x.EARLY_REDEMPTION_FLAG = y.EARLY_REDEMPTION_FLAG,
        x.OVD_YN_STATUS = y.OVD_YN_STATUS,
        x.OVD_30D_YN_STATUS = y.OVD_30D_YN_STATUS,
        x.AR_NBR = y.AR_NBR,
        x.ACCOUNT_STATUS = y.ACCOUNT_STATUS,
        x.LIMIT_AMT = y.LIMIT_AMT
    FROM EWS.[dbo].[IFRS9_CTR_CL_CL020] x
    INNER JOIN #tmp_ctr_cl y
    ON (x.process_date = EOMONTH(y.process_date) AND x.contract_id = y.contract_id)
    WHERE x.PROCESS_DATE = @run_date;

    -- Update CG_SEGMENT
    SELECT DISTINCT CUSTOMER_ID, CG_SEGMENT
    INTO #tmp_cg_segment
    FROM ews.dbo.IFRS9_CUST_INFO
    WHERE CG_SEGMENT IS NOT NULL
    AND PROCESS_DATE = @run_date;

    UPDATE x
    SET x.CUS_SEGMENT = y.CG_SEGMENT
    FROM EWS.[dbo].[IFRS9_CTR_CL_CL020] x
    INNER JOIN #tmp_cg_segment y
    ON (x.CUSTOMER_ID = y.CUSTOMER_ID)
    WHERE x.PROCESS_DATE = @run_date;

    -- Clean up LIMIT_ID
    UPDATE EWS.[dbo].[IFRS9_CTR_CL_CL020]
    SET LIMIT_ID = LEFT(LIMIT_ID, LEN(LIMIT_ID) - 1)
    WHERE RIGHT(LIMIT_ID, 1) = '/'
    AND PROCESS_DATE = @run_date;
END
GO
