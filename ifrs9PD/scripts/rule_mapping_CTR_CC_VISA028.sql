USE [EWS]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Drop the existing procedure if it exists
IF OBJECT_ID('[dbo].[rule_mapping_CTR_CC_VISA028]', 'P') IS NOT NULL
    DROP PROCEDURE [dbo].[rule_mapping_CTR_CC_VISA028]
GO

CREATE PROC [dbo].[rule_mapping_CTR_CC_VISA028]
    @run_date DATE = NULL
AS
BEGIN
    -- Set default run date if not provided
    IF @run_date IS NULL
        SET @run_date = EOMONTH(DATEADD(MONTH, -1, GETDATE()));

    -- Delete existing records for the given date
    DELETE FROM EWS.[dbo].[IFRS9_CTR_CC_VISA028]
    WHERE PROCESS_DATE = @run_date;

    -- Insert data from VISA028_CLEAN into IFRS9_CTR_CC_VISA028
    INSERT INTO EWS.[dbo].[IFRS9_CTR_CC_VISA028] (
        process_date, CONTRACT_ID, CUSTOMER_ID
    )
    SELECT DISTINCT 
        process_date, 
        CONTRACT_ID = REPLACE(CLIENT_CODE, 'A', ''),
        CUSTOMER_ID = REPLACE(CUST_NO, 'A', '')
    FROM [DATA].[dbo].[VISA028_CLEAN]
    WHERE PROCESS_DATE = @run_date;

    -- Update IFRS9_CTR_CC_VISA028 with data from VISA028_CLEAN
    UPDATE x
    SET 
        x.MUD_CODE = 'CC',
        x.CONTRACT_ID = REPLACE(y.CLIENT_CODE, 'A', ''),
        x.CUSTOMER_ID = REPLACE(y.CUST_NO, 'A', ''),
        x.BRANCH_CODE = CONCAT(REPLICATE(0, 3 - LEN(y.BRANCH_CODE)), y.BRANCH_CODE),
        x.CURRENCY = 'VND',
        x.EXR_AT_DISBURSEMENT = '1',
        x.ACCOUNT_STATUS = y.HOT_CARD,
        x.PRODUCT_GROUP = 'On_B',
        x.PRODUCT_CODE = y.TYPE,
        x.DISBURSEMENT_DATE = y.OPENING_DATE,
        x.EXPIRY_DATE = y.EXPIRE_DATE,
        x.OS_AMOUNT = y.Duno,
        x.DPD = y.OD_COUNT_DAYS,
        x.NHOM_NO_CONTRACT = y.NHOM_NO_NEW,
        x.LIMIT_AMT = y.CREDIT_LIMIT,
        x.Duno_saoke = y.Duno_saoke,
        x.Duno_vuotHM = y.Duno_vuotHM
    FROM EWS.[dbo].[IFRS9_CTR_CC_VISA028] x
    INNER JOIN [DATA].[dbo].[VISA028_CLEAN] y
    ON x.process_date = y.process_date 
    AND x.CONTRACT_ID = REPLACE(y.CLIENT_CODE, 'A', '')
    WHERE x.PROCESS_DATE = @run_date;

    -- Adjust specific CUSTOMER_ID values
    UPDATE EWS.[dbo].[IFRS9_CTR_CC_VISA028]
    SET CUSTOMER_ID = RIGHT(CUSTOMER_ID, 8)
    WHERE CUSTOMER_ID LIKE '%00064889%';

    -- Update IFRS9_CTR_CC_VISA028 with data from IFRS9_CTR_CC
    UPDATE x
    SET 
        x.RPT_DT_DIM_ID = y.RPT_DT_DIM_ID,
        x.CUSTOMER_INDUSTRY = y.CUSTOMER_INDUSTRY,
        x.ALTERNATIVE_ACCOUNT_NUMBER = y.ALTERNATIVE_ACCOUNT_NUMBER,
        x.CUS_SEGMENT = y.CUS_SEGMENT,
        x.GL_CODE = y.GL_CODE,
        x.INTEREST_RATE = y.INTEREST_RATE,
        x.OUTSTANDING_AVG_MONTH = y.OUTSTANDING_AVG_MONTH,
        x.PRINCIPAL_OS_AMOUNT = y.PRINCIPAL_OS_AMOUNT,
        x.PRINCIPAL_OVERDUE_AMOUNT = y.PRINCIPAL_OVERDUE_AMOUNT,
        x.INTEREST_OVERDUE_AMOUNT = y.INTEREST_OVERDUE_AMOUNT,
        x.INTEREST_OS_AMOUNT = y.INTEREST_OS_AMOUNT,
        x.INTEREST_PENALTY_AMOUNT = y.INTEREST_PENALTY_AMOUNT,
        x.PRINCIPAL_PAYMENT_FREQUENCY = y.PRINCIPAL_PAYMENT_FREQUENCY,
        x.INTEREST_PAYMENT_FREQUENCY = y.INTEREST_PAYMENT_FREQUENCY,
        x.NEXT_PRINCIPAL_PAYMENT_AMOUNT = y.NEXT_PRINCIPAL_PAYMENT_AMOUNT,
        x.NEXT_INTEREST_PAYMENT_AMOUNT = y.NEXT_INTEREST_PAYMENT_AMOUNT,
        x.NEXT_PRINCIPAL_PAYMENT_DATE = y.NEXT_PRINCIPAL_PAYMENT_DATE,
        x.NEXT_INTEREST_PAYMENT_DATE = y.NEXT_INTEREST_PAYMENT_DATE,
        x.OVD_PRIN_START_DATE = y.OVD_PRIN_START_DATE,
        x.OVD_INT_START_DATE = y.OVD_INT_START_DATE,
        x.MAX_DPD = y.MAX_DPD,
        x.DISBURSEMENT_AMOUNT = y.DISBURSEMENT_AMOUNT,
        x.LIMIT_ID = y.LIMIT_ID,
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
        x.ACCOUNT_STATUS = y.ACCOUNT_STATUS
    FROM EWS.[dbo].[IFRS9_CTR_CC_VISA028] x
    INNER JOIN [dbo].[IFRS9_CTR_CC] y
    ON x.process_date = y.process_date 
    AND x.CUSTOMER_ID = y.CUSTOMER_ID
    WHERE x.PROCESS_DATE = @run_date;

    -- Delete invalid rows
    DELETE FROM EWS.[dbo].[IFRS9_CTR_CC_VISA028]
    WHERE LEN(CUSTOMER_ID) != 8 
        OR CUSTOMER_ID LIKE '%X%' 
        OR CUSTOMER_ID = '' 
        OR BRANCH_CODE = '555';

    -- Update CUS_SEGMENT from IFRS9_CUST_INFO
    UPDATE x
    SET x.CUS_SEGMENT = y.CG_SEGMENT
    FROM EWS.[dbo].[IFRS9_CTR_CC_VISA028] x
    INNER JOIN [dbo].[IFRS9_CUST_INFO] y
    ON x.process_date = y.process_date 
    AND x.CUSTOMER_ID = y.CUSTOMER_ID
    WHERE x.PROCESS_DATE = @run_date;
END
GO
