/*
 * Author: BinhNN2
 * Database: DB2
 * Project: CIC LONGLIST
 * PRIMARY KEY: RPT_DT, MSPHIEU, MA_CIC
 * First date: 2023-11-24
 * Update: 2024/07: change DATE_MSP to RPT_DT -- TO_DATE(SUBSTR(MSPHIEU, 4, 6), 'YYMMDD')  AS DATE_MSP,
 		   2024/07: add start_date, end_date
 */
CREATE OR REPLACE PROCEDURE CIC_RB06_DUNO_12M(
    IN start_date DATE,
    IN end_date DATE
)
LANGUAGE SQL
BEGIN
    
    -- Drop the global temporary table if it exists
    DROP TABLE IF EXISTS SESSION.tmp_filter_date;

    -- Create the global temporary table
    DECLARE GLOBAL TEMPORARY TABLE SESSION.tmp_filter_date (
        RPT_DT DATE,
        MSPHIEU VARCHAR(50),
        MA_CIC VARCHAR(50),
        THANG DATE,
        TONGDUNO DECIMAL(18, 2),
        DUNOVAY DECIMAL(18, 2),
        DUNOTHE DECIMAL(18, 2)
    ) WITH REPLACE ON COMMIT PRESERVE ROWS;

    -- Insert data into the global temporary table
    INSERT INTO SESSION.tmp_filter_date
    SELECT DISTINCT 
        RPT_DT,
        MSPHIEU,    
        MA_CIC,
        TO_DATE(THANG, 'YYYYMM') AS THANG,    
        COALESCE(CAST(TONGDUNO AS DECIMAL(18, 2)), 0.0) AS TONGDUNO,
        COALESCE(CAST(DUNOVAY AS DECIMAL(18, 2)), 0.0) AS DUNOVAY,
        COALESCE(CAST(DUNOTHE AS DECIMAL(18, 2)), 0.0) AS DUNOTHE
    FROM
        CSO.CIC_RB06_DUNO_12M     
    WHERE TO_DATE(RPT_DT, 'YYYYMMDD') BETWEEN start_date AND end_date;

	
	DROP TABLE IF EXISTS SESSION.tmp_CIC_RB06_DUNO_12M;

	DECLARE GLOBAL TEMPORARY TABLE SESSION.tmp_CIC_RB06_DUNO_12M AS (
	WITH grp as(
	SELECT RPT_DT
		, MSPHIEU
		, MA_CIC
		, THANG
		, sum(TONGDUNO) TONGDUNO
		, sum(DUNOVAY) DUNOVAY 
		, sum(DUNOTHE) DUNOTHE
	FROM SESSION.tmp_filter_date
	GROUP BY RPT_DT, MSPHIEU, MA_CIC, THANG
	),
	lag_tbl as(
	-- do da kiem tra cac quan sat deu co 12 dong, nen dung duoc luon ham lag
	SELECT *
		,ROW_NUMBER() OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ) AS RN 
		,LEAD(TONGDUNO,  1) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS TONGDUNO_L1M
		,LEAD(TONGDUNO,  2) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS TONGDUNO_L3M
		,LEAD(TONGDUNO,  5) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS TONGDUNO_L6M
		,LEAD(TONGDUNO, 11) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS TONGDUNO_L12M
		,LEAD(DUNOVAY,   1) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOVAY_L1M
		,LEAD(DUNOVAY,   2) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOVAY_L3M
		,LEAD(DUNOVAY,   5) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOVAY_L6M
		,LEAD(DUNOVAY,  11) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOVAY_L12M
		,LEAD(DUNOTHE,   1) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOTHE_L1M
		,LEAD(DUNOTHE,   2) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOTHE_L3M
		,LEAD(DUNOTHE,   5) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOTHE_L6M
		,LEAD(DUNOTHE,  11) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG DESC) AS DUNOTHE_L12M 
		,SUM(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS SUM_TONGDUNO_L3M
		,SUM(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS SUM_TONGDUNO_L6M
		,SUM(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS SUM_TONGDUNO_L12M
		,SUM(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS SUM_DUNOVAY_L3M
		,SUM(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS SUM_DUNOVAY_L6M
		,SUM(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS SUM_DUNOVAY_L12M
		,SUM(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS SUM_DUNOTHE_L3M
		,SUM(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS SUM_DUNOTHE_L6M
		,SUM(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS SUM_DUNOTHE_L12M
		,MIN(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MIN_TONGDUNO_L3M
		,MIN(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS MIN_TONGDUNO_L6M
		,MIN(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS MIN_TONGDUNO_L12M
		,MIN(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MIN_DUNOVAY_L3M
		,MIN(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS MIN_DUNOVAY_L6M
		,MIN(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS MIN_DUNOVAY_L12M
		,MIN(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MIN_DUNOTHE_L3M
		,MIN(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS MIN_DUNOTHE_L6M
		,MIN(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS MIN_DUNOTHE_L12M
		,MAX(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MAX_TONGDUNO_L3M
		,MAX(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS MAX_TONGDUNO_L6M
		,MAX(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS MAX_TONGDUNO_L12M
		,MAX(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MAX_DUNOVAY_L3M
		,MAX(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS MAX_DUNOVAY_L6M
		,MAX(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS MAX_DUNOVAY_L12M
		,MAX(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MAX_DUNOTHE_L3M
		,MAX(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS MAX_DUNOTHE_L6M
		,MAX(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS MAX_DUNOTHE_L12M
		,AVG(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS AVG_TONGDUNO_L3M
		,AVG(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS AVG_TONGDUNO_L6M
		,AVG(TONGDUNO) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS AVG_TONGDUNO_L12M
		,AVG(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS AVG_DUNOVAY_L3M
		,AVG(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS AVG_DUNOVAY_L6M
		,AVG(DUNOVAY) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS AVG_DUNOVAY_L12M
		,AVG(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS AVG_DUNOTHE_L3M
		,AVG(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS AVG_DUNOTHE_L6M
		,AVG(DUNOTHE) OVER (PARTITION BY  RPT_DT, MSPHIEU, MA_CIC ORDER BY THANG ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS AVG_DUNOTHE_L12M
		FROM grp 
	)
	SELECT *
		,TONGDUNO/NULLIF(TONGDUNO_L1M, 0) - 1 AS PERC_TONGDUNO_L1M 
		,TONGDUNO/NULLIF(TONGDUNO_L3M, 0) - 1 AS PERC_TONGDUNO_L3M
		,TONGDUNO/NULLIF(TONGDUNO_L6M, 0) - 1 AS PERC_TONGDUNO_L6M
		,TONGDUNO/NULLIF(TONGDUNO_L12M,0) - 1 AS PERC_TONGDUNO_L12M
		,DUNOVAY /NULLIF(DUNOVAY_L1M,  0) - 1 AS PERC_DUNOVAY_L1M
		,DUNOVAY /NULLIF(DUNOVAY_L3M,  0) - 1 AS PERC_DUNOVAY_L3M
		,DUNOVAY /NULLIF(DUNOVAY_L6M,  0) - 1 AS PERC_DUNOVAY_L6M
		,DUNOVAY /NULLIF(DUNOVAY_L12M, 0) - 1 AS PERC_DUNOVAY_L12M
		,DUNOTHE /NULLIF(DUNOTHE_L1M,  0) - 1 AS PERC_DUNOTHE_L1M
		,DUNOTHE /NULLIF(DUNOTHE_L3M,  0) - 1 AS PERC_DUNOTHE_L3M
		,DUNOTHE /NULLIF(DUNOTHE_L6M,  0) - 1 AS PERC_DUNOTHE_L6M
		,DUNOTHE /NULLIF(DUNOTHE_L12M, 0) - 1 AS PERC_DUNOTHE_L12M
	FROM lag_tbl
	WHERE rn <= 12
	) WITH DATA ON COMMIT PRESERVE ROWS ;

-- Tao form bang
-- nhung cot lay 1 gia tri gan nhat
-------------------------------------------------------------
	DROP TABLE IF EXISTS SESSION.tmp_rn1;	
	DECLARE GLOBAL TEMPORARY TABLE SESSION.tmp_rn1 AS
	-- INSERT INTO tmp_rn1
	(SELECT
		RPT_DT, MSPHIEU, MA_CIC,
		COALESCE(DUNOVAY, 0) - COALESCE(DUNOVAY_L3M, 0) AS LOAN_OS_CHANGE_VALUE_3M,
		COALESCE(DUNOVAY, 0) - COALESCE(DUNOVAY_L6M, 0) AS LOAN_OS_CHANGE_VALUE_6M,
		COALESCE(DUNOVAY, 0) - COALESCE(DUNOVAY_L12M, 0) AS LOAN_OS_CHANGE_VALUE_12M,
		COALESCE(PERC_DUNOVAY_L3M, 0) AS LOAN_OS_CHANGE_RATIO_3M,
		COALESCE(PERC_DUNOVAY_L6M, 0) AS LOAN_OS_CHANGE_RATIO_6M,
		COALESCE(PERC_DUNOVAY_L12M, 0) AS LOAN_OS_CHANGE_RATIO_12M,
		COALESCE(TONGDUNO, 0) - COALESCE(TONGDUNO_L3M, 0) AS LOAN_CC_OS_CHANGE_VALUE_3M,
		COALESCE(TONGDUNO, 0) - COALESCE(TONGDUNO_L6M, 0) AS LOAN_CC_OS_CHANGE_VALUE_6M,
		COALESCE(TONGDUNO, 0) - COALESCE(TONGDUNO_L12M, 0) AS LOAN_CC_OS_CHANGE_VALUE_12M,
		COALESCE(PERC_TONGDUNO_L3M, 0) AS LOAN_CC_OS_CHANGE_RATIO_3M,
		COALESCE(PERC_TONGDUNO_L6M, 0) AS LOAN_CC_OS_CHANGE_RATIO_6M,
		COALESCE(PERC_TONGDUNO_L12M, 0) AS LOAN_CC_OS_CHANGE_RATIO_12M,
		COALESCE(DUNOTHE, 0) - COALESCE(DUNOTHE_L3M, 0) AS CC_OS_CHANGE_VALUE_3M,
		COALESCE(DUNOTHE, 0) - COALESCE(DUNOTHE_L6M, 0) AS CC_OS_CHANGE_VALUE_6M,
		COALESCE(DUNOTHE, 0) - COALESCE(DUNOTHE_L12M, 0) AS CC_OS_CHANGE_VALUE_12M,
		COALESCE(PERC_DUNOTHE_L3M, 0) AS CC_OS_CHANGE_RATIO_3M,
		COALESCE(PERC_DUNOTHE_L6M, 0) AS CC_OS_CHANGE_RATIO_6M,
		COALESCE(PERC_DUNOTHE_L12M, 0) AS CC_OS_CHANGE_RATIO_12M
		
		,SUM_DUNOVAY_L3M /NULLIF(SUM_TONGDUNO_L3M, 0) AS LOAN_OS_RATIO_L3M
		,SUM_DUNOVAY_L6M /NULLIF(SUM_TONGDUNO_L6M, 0) AS LOAN_OS_RATIO_L6M
		,SUM_DUNOVAY_L12M/NULLIF(SUM_TONGDUNO_L12M,0) AS LOAN_OS_RATIO_L12M
			
		,(COALESCE(MAX_DUNOVAY_L3M, 0) - COALESCE(MIN_DUNOVAY_L3M, 0))/NULLIF(AVG_DUNOVAY_L3M, 0) AS LOAN_OS_VOLATILE_L3M
		,(COALESCE(MAX_DUNOVAY_L6M, 0) - COALESCE(MIN_DUNOVAY_L6M, 0))/NULLIF(AVG_DUNOVAY_L6M, 0) AS LOAN_OS_VOLATILE_L6M
		,(COALESCE(MAX_DUNOVAY_L12M,0) - COALESCE(MIN_DUNOVAY_L12M,0))/NULLIF(AVG_DUNOVAY_L12M,0) AS LOAN_OS_VOLATILE_L12M
						
		,SUM_DUNOTHE_L3M /NULLIF(SUM_TONGDUNO_L3M, 0) AS CC_OS_RATIO_L3M
		,SUM_DUNOTHE_L6M /NULLIF(SUM_TONGDUNO_L6M, 0) AS CC_OS_RATIO_L6M
		,SUM_DUNOTHE_L12M/NULLIF(SUM_TONGDUNO_L12M,0) AS CC_OS_RATIO_L12M
			
		,(COALESCE(MAX_DUNOTHE_L3M, 0) - COALESCE(MIN_DUNOTHE_L3M, 0))/NULLIF(AVG_DUNOTHE_L3M, 0) AS CC_OS_VOLATILE_L3M
		,(COALESCE(MAX_DUNOTHE_L6M, 0) - COALESCE(MIN_DUNOTHE_L6M, 0))/NULLIF(AVG_DUNOTHE_L6M, 0) AS CC_OS_VOLATILE_L6M
		,(COALESCE(MAX_DUNOTHE_L12M,0) - COALESCE(MIN_DUNOTHE_L12M,0))/NULLIF(AVG_DUNOTHE_L12M,0) AS CC_OS_VOLATILE_L12M   		
			
	FROM SESSION.tmp_CIC_RB06_DUNO_12M
	WHERE rn = 12
	AND RPT_DT IS NOT NULL 
	AND MSPHIEU IS NOT NULL 
	AND MA_CIC IS NOT null
	) WITH DATA ORGANIZE BY ROW
	;

/*
	ALTER TABLE tmp_rn1
	ALTER COLUMN RPT_DT SET NOT NULL;
	ALTER TABLE tmp_rn1
	ALTER COLUMN MA_CIC SET NOT NULL;
	ALTER TABLE tmp_rn1
	ADD PRIMARY KEY (RPT_DT, MSPHIEU, MA_CIC);
*/
DROP TABLE IF EXISTS SESSION.tmp_grp12;

DECLARE GLOBAL TEMPORARY TABLE SESSION.tmp_grp12 AS (
SELECT
        RPT_DT, MSPHIEU, MA_CIC
        ,REGR_SLOPE(DUNOVAY, RN) AS LOAN_OS_SLOPE_12M 
   		,sum(CASE when DUNOVAY > DUNOVAY_L1M AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_MONTHS_L3M  		
   		,sum(CASE when DUNOVAY > DUNOVAY_L1M AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_MONTHS_L6M
   		,sum(CASE when DUNOVAY > DUNOVAY_L1M 			  THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_MONTHS_L12M
   		,sum(CASE when DUNOVAY < DUNOVAY_L1M AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_MONTHS_L3M  		
   		,sum(CASE when DUNOVAY < DUNOVAY_L1M AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_MONTHS_L6M
   		,sum(CASE when DUNOVAY < DUNOVAY_L1M 			  THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_MONTHS_L12M
   		
   		,sum(CASE when TONGDUNO > TONGDUNO_L1M AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_INCR_MONTHS_L3M  
   		,sum(CASE when TONGDUNO > TONGDUNO_L1M AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_INCR_MONTHS_L6M
   		,sum(CASE when TONGDUNO > TONGDUNO_L1M 				THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_INCR_MONTHS_L12M
   		,sum(CASE when TONGDUNO < TONGDUNO_L1M AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_DECR_MONTHS_L3M  
   		,sum(CASE when TONGDUNO < TONGDUNO_L1M AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_DECR_MONTHS_L6M
   		,sum(CASE when TONGDUNO < TONGDUNO_L1M 				THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_DECR_MONTHS_L12M
   		
   		,sum(CASE when DUNOVAY = 0 AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_NULL_MONTHS_L3M  
   		,sum(CASE when DUNOVAY = 0 AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_NULL_MONTHS_L6M
   		,sum(CASE when DUNOVAY = 0 				THEN 1 ELSE 0 end ) AS LOAN_OS_NULL_MONTHS_L12M
   		,sum(CASE when TONGDUNO= 0 AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_NULL_MONTHS_L3M  
   		,sum(CASE when TONGDUNO= 0 AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_NULL_MONTHS_L6M
   		,sum(CASE when TONGDUNO= 0 				THEN 1 ELSE 0 end ) AS LOAN_CC_OS_TOTAL_NULL_MONTHS_L12M
   		
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.25 AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_25_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.25 AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_25_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.25 			 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_25_L12M
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.5 	AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_50_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.5 	AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_50_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.5  			 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_50_L12M
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.75 AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_75_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.75 AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_75_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M >= 0.75 			 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_75_L12M
   		,sum(CASE when PERC_DUNOVAY_L1M >= 1 	AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_100_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M >= 1 	AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_100_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M >= 1 				 THEN 1 ELSE 0 end ) AS LOAN_OS_INCR_PER_MONTHS_100_L12M
   		
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.25  AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_25_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.25  AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_25_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.25  			 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_25_L12M
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.5 	AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_50_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.5 	AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_50_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.5   			 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_50_L12M
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.75  AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_75_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.75  AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_75_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M < 0.75  			 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_75_L12M
   		,sum(CASE when PERC_DUNOVAY_L1M < 1 	AND rn >= 10 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_100_L3M  
   		,sum(CASE when PERC_DUNOVAY_L1M < 1 	AND rn >= 7  THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_100_L6M
   		,sum(CASE when PERC_DUNOVAY_L1M < 1 				 THEN 1 ELSE 0 end ) AS LOAN_OS_DECR_PER_MONTHS_100_L12M
   		    		
   		,REGR_SLOPE(DUNOTHE, RN) AS CC_OS_SLOPE_12M
   		,sum(CASE when DUNOTHE > DUNOTHE_L1M AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_INCR_MONTHS_L3M  		
   		,sum(CASE when DUNOTHE > DUNOTHE_L1M AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_INCR_MONTHS_L6M
   		,sum(CASE when DUNOTHE > DUNOTHE_L1M 			  THEN 1 ELSE 0 end ) AS CC_OS_INCR_MONTHS_L12M
   		,sum(CASE when DUNOTHE < DUNOTHE_L1M AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_DECR_MONTHS_L3M  		
   		,sum(CASE when DUNOTHE < DUNOTHE_L1M AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_DECR_MONTHS_L6M
   		,sum(CASE when DUNOTHE < DUNOTHE_L1M 			  THEN 1 ELSE 0 end ) AS CC_OS_DECR_MONTHS_L12M
		
   		,sum(CASE when DUNOTHE = 0 AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_NULL_MONTHS_L3M  
   		,sum(CASE when DUNOTHE = 0 AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_NULL_MONTHS_L6M
   		,sum(CASE when DUNOTHE = 0 				THEN 1 ELSE 0 end ) 			  AS CC_OS_NULL_MONTHS_L12M
   		
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.25 AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_25_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.25 AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_25_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.25 			 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_25_L12M
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.5 	AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_50_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.5 	AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_50_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.5  			 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_50_L12M
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.75 AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_75_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.75 AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_75_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M >= 0.75 			 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_75_L12M
   		,sum(CASE when PERC_DUNOTHE_L1M >= 1 	AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_100_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M >= 1 	AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_100_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M >= 1 				 THEN 1 ELSE 0 end ) AS CC_OS_INCR_PER_MONTHS_100_L12M
   		
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.25  AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_25_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.25  AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_25_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.25  			 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_25_L12M
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.5 	AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_50_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.5 	AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_50_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.5   			 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_50_L12M
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.75  AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_75_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.75  AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_75_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M < 0.75  			 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_75_L12M
   		,sum(CASE when PERC_DUNOTHE_L1M < 1 	AND rn >= 10 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_100_L3M  
   		,sum(CASE when PERC_DUNOTHE_L1M < 1 	AND rn >= 7  THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_100_L6M
   		,sum(CASE when PERC_DUNOTHE_L1M < 1 				 THEN 1 ELSE 0 end ) AS CC_OS_DECR_PER_MONTHS_100_L12M
   		  		
    FROM SESSION.tmp_CIC_RB06_DUNO_12M
    GROUP BY RPT_DT, MSPHIEU, MA_CIC
   )
  WITH DATA ORGANIZE BY ROW;
 
   
--------------------------------------------------------------------------------
-- Update  tmp_rn1
UPDATE TPBRM1.CIC_FEATURE_STORE_KHCN x
SET 
x.LOAN_OS_CHANGE_VALUE_3M = y.LOAN_OS_CHANGE_VALUE_3M
,x.LOAN_OS_CHANGE_VALUE_6M = y.LOAN_OS_CHANGE_VALUE_6M
,x.LOAN_OS_CHANGE_VALUE_12M = y.LOAN_OS_CHANGE_VALUE_12M
,x.LOAN_OS_CHANGE_RATIO_3M = y.LOAN_OS_CHANGE_RATIO_3M
,x.LOAN_OS_CHANGE_RATIO_6M = y.LOAN_OS_CHANGE_RATIO_6M
,x.LOAN_OS_CHANGE_RATIO_12M = y.LOAN_OS_CHANGE_RATIO_12M
,x.LOAN_OS_RATIO_L3M = y.LOAN_OS_RATIO_L3M
,x.LOAN_OS_RATIO_L6M = y.LOAN_OS_RATIO_L6M
,x.LOAN_OS_RATIO_L12M = y.LOAN_OS_RATIO_L12M
,x.LOAN_OS_VOLATILE_L3M = y.LOAN_OS_VOLATILE_L3M
,x.LOAN_OS_VOLATILE_L6M = y.LOAN_OS_VOLATILE_L6M
,x.LOAN_OS_VOLATILE_L12M = y.LOAN_OS_VOLATILE_L12M
,x.LOAN_CC_OS_CHANGE_VALUE_3M = y.LOAN_CC_OS_CHANGE_VALUE_3M
,x.LOAN_CC_OS_CHANGE_VALUE_6M = y.LOAN_CC_OS_CHANGE_VALUE_6M
,x.LOAN_CC_OS_CHANGE_VALUE_12M = y.LOAN_CC_OS_CHANGE_VALUE_12M
,x.LOAN_CC_OS_CHANGE_RATIO_3M = y.LOAN_CC_OS_CHANGE_RATIO_3M
,x.LOAN_CC_OS_CHANGE_RATIO_6M = y.LOAN_CC_OS_CHANGE_RATIO_6M
,x.LOAN_CC_OS_CHANGE_RATIO_12M = y.LOAN_CC_OS_CHANGE_RATIO_12M
,x.CC_OS_CHANGE_VALUE_3M = y.CC_OS_CHANGE_VALUE_3M
,x.CC_OS_CHANGE_VALUE_6M = y.CC_OS_CHANGE_VALUE_6M
,x.CC_OS_CHANGE_VALUE_12M = y.CC_OS_CHANGE_VALUE_12M
,x.CC_OS_CHANGE_RATIO_3M = y.CC_OS_CHANGE_RATIO_3M
,x.CC_OS_CHANGE_RATIO_6M = y.CC_OS_CHANGE_RATIO_6M
,x.CC_OS_CHANGE_RATIO_12M = y.CC_OS_CHANGE_RATIO_12M
,x.CC_OS_RATIO_L3M = y.CC_OS_RATIO_L3M
,x.CC_OS_RATIO_L6M = y.CC_OS_RATIO_L6M
,x.CC_OS_RATIO_L12M = y.CC_OS_RATIO_L12M
,x.CC_OS_VOLATILE_L3M = y.CC_OS_VOLATILE_L3M
,x.CC_OS_VOLATILE_L6M = y.CC_OS_VOLATILE_L6M
,x.CC_OS_VOLATILE_L12M = y.CC_OS_VOLATILE_L12M
FROM SESSION.tmp_rn1 y
WHERE x.RPT_DT = y.RPT_DT
	AND x.MSPHIEU = y.MSPHIEU
    AND x.MA_CIC = y.MA_CIC;
      
--------------
UPDATE TPBRM1.CIC_FEATURE_STORE_KHCN x
SET 
x.LOAN_OS_SLOPE_12M = y.LOAN_OS_SLOPE_12M
,x.LOAN_OS_INCR_MONTHS_L3M = y.LOAN_OS_INCR_MONTHS_L3M
,x.LOAN_OS_INCR_MONTHS_L6M = y.LOAN_OS_INCR_MONTHS_L6M
,x.LOAN_OS_INCR_MONTHS_L12M = y.LOAN_OS_INCR_MONTHS_L12M
,x.LOAN_OS_DECR_MONTHS_L3M = y.LOAN_OS_DECR_MONTHS_L3M
,x.LOAN_OS_DECR_MONTHS_L6M = y.LOAN_OS_DECR_MONTHS_L6M
,x.LOAN_OS_DECR_MONTHS_L12M = y.LOAN_OS_DECR_MONTHS_L12M
,x.LOAN_CC_OS_TOTAL_INCR_MONTHS_L3M = y.LOAN_CC_OS_TOTAL_INCR_MONTHS_L3M
,x.LOAN_CC_OS_TOTAL_INCR_MONTHS_L6M = y.LOAN_CC_OS_TOTAL_INCR_MONTHS_L6M
,x.LOAN_CC_OS_TOTAL_INCR_MONTHS_L12M = y.LOAN_CC_OS_TOTAL_INCR_MONTHS_L12M
,x.LOAN_CC_OS_TOTAL_DECR_MONTHS_L3M = y.LOAN_CC_OS_TOTAL_DECR_MONTHS_L3M
,x.LOAN_CC_OS_TOTAL_DECR_MONTHS_L6M = y.LOAN_CC_OS_TOTAL_DECR_MONTHS_L6M
,x.LOAN_CC_OS_TOTAL_DECR_MONTHS_L12M = y.LOAN_CC_OS_TOTAL_DECR_MONTHS_L12M
,x.LOAN_OS_NULL_MONTHS_L3M = y.LOAN_OS_NULL_MONTHS_L3M
,x.LOAN_OS_NULL_MONTHS_L6M = y.LOAN_OS_NULL_MONTHS_L6M
,x.LOAN_OS_NULL_MONTHS_L12M = y.LOAN_OS_NULL_MONTHS_L12M
,x.LOAN_CC_OS_TOTAL_NULL_MONTHS_L3M = y.LOAN_CC_OS_TOTAL_NULL_MONTHS_L3M
,x.LOAN_CC_OS_TOTAL_NULL_MONTHS_L6M = y.LOAN_CC_OS_TOTAL_NULL_MONTHS_L6M
,x.LOAN_CC_OS_TOTAL_NULL_MONTHS_L12M = y.LOAN_CC_OS_TOTAL_NULL_MONTHS_L12M
,x.LOAN_OS_INCR_PER_MONTHS_25_L3M = y.LOAN_OS_INCR_PER_MONTHS_25_L3M
,x.LOAN_OS_INCR_PER_MONTHS_25_L6M = y.LOAN_OS_INCR_PER_MONTHS_25_L6M
,x.LOAN_OS_INCR_PER_MONTHS_25_L12M = y.LOAN_OS_INCR_PER_MONTHS_25_L12M
,x.LOAN_OS_INCR_PER_MONTHS_50_L3M = y.LOAN_OS_INCR_PER_MONTHS_50_L3M
,x.LOAN_OS_INCR_PER_MONTHS_50_L6M = y.LOAN_OS_INCR_PER_MONTHS_50_L6M
,x.LOAN_OS_INCR_PER_MONTHS_50_L12M = y.LOAN_OS_INCR_PER_MONTHS_50_L12M
,x.LOAN_OS_INCR_PER_MONTHS_75_L3M = y.LOAN_OS_INCR_PER_MONTHS_75_L3M
,x.LOAN_OS_INCR_PER_MONTHS_75_L6M = y.LOAN_OS_INCR_PER_MONTHS_75_L6M
,x.LOAN_OS_INCR_PER_MONTHS_75_L12M = y.LOAN_OS_INCR_PER_MONTHS_75_L12M
,x.LOAN_OS_INCR_PER_MONTHS_100_L3M = y.LOAN_OS_INCR_PER_MONTHS_100_L3M
,x.LOAN_OS_INCR_PER_MONTHS_100_L6M = y.LOAN_OS_INCR_PER_MONTHS_100_L6M
,x.LOAN_OS_INCR_PER_MONTHS_100_L12M = y.LOAN_OS_INCR_PER_MONTHS_100_L12M
,x.LOAN_OS_DECR_PER_MONTHS_25_L3M = y.LOAN_OS_DECR_PER_MONTHS_25_L3M
,x.LOAN_OS_DECR_PER_MONTHS_25_L6M = y.LOAN_OS_DECR_PER_MONTHS_25_L6M
,x.LOAN_OS_DECR_PER_MONTHS_25_L12M = y.LOAN_OS_DECR_PER_MONTHS_25_L12M
,x.LOAN_OS_DECR_PER_MONTHS_50_L3M = y.LOAN_OS_DECR_PER_MONTHS_50_L3M
,x.LOAN_OS_DECR_PER_MONTHS_50_L6M = y.LOAN_OS_DECR_PER_MONTHS_50_L6M
,x.LOAN_OS_DECR_PER_MONTHS_50_L12M = y.LOAN_OS_DECR_PER_MONTHS_50_L12M
,x.LOAN_OS_DECR_PER_MONTHS_75_L3M = y.LOAN_OS_DECR_PER_MONTHS_75_L3M
,x.LOAN_OS_DECR_PER_MONTHS_75_L6M = y.LOAN_OS_DECR_PER_MONTHS_75_L6M
,x.LOAN_OS_DECR_PER_MONTHS_75_L12M = y.LOAN_OS_DECR_PER_MONTHS_75_L12M
,x.LOAN_OS_DECR_PER_MONTHS_100_L3M = y.LOAN_OS_DECR_PER_MONTHS_100_L3M
,x.LOAN_OS_DECR_PER_MONTHS_100_L6M = y.LOAN_OS_DECR_PER_MONTHS_100_L6M
,x.LOAN_OS_DECR_PER_MONTHS_100_L12M = y.LOAN_OS_DECR_PER_MONTHS_100_L12M
,x.CC_OS_SLOPE_12M = y.CC_OS_SLOPE_12M
,x.CC_OS_INCR_MONTHS_L3M = y.CC_OS_INCR_MONTHS_L3M
,x.CC_OS_INCR_MONTHS_L6M = y.CC_OS_INCR_MONTHS_L6M
,x.CC_OS_INCR_MONTHS_L12M = y.CC_OS_INCR_MONTHS_L12M
,x.CC_OS_DECR_MONTHS_L3M = y.CC_OS_DECR_MONTHS_L3M
,x.CC_OS_DECR_MONTHS_L6M = y.CC_OS_DECR_MONTHS_L6M
,x.CC_OS_DECR_MONTHS_L12M = y.CC_OS_DECR_MONTHS_L12M
,x.CC_OS_NULL_MONTHS_L3M = y.CC_OS_NULL_MONTHS_L3M
,x.CC_OS_NULL_MONTHS_L6M = y.CC_OS_NULL_MONTHS_L6M
,x.CC_OS_NULL_MONTHS_L12M = y.CC_OS_NULL_MONTHS_L12M
,x.CC_OS_INCR_PER_MONTHS_25_L3M = y.CC_OS_INCR_PER_MONTHS_25_L3M
,x.CC_OS_INCR_PER_MONTHS_25_L6M = y.CC_OS_INCR_PER_MONTHS_25_L6M
,x.CC_OS_INCR_PER_MONTHS_25_L12M = y.CC_OS_INCR_PER_MONTHS_25_L12M
,x.CC_OS_INCR_PER_MONTHS_50_L3M = y.CC_OS_INCR_PER_MONTHS_50_L3M
,x.CC_OS_INCR_PER_MONTHS_50_L6M = y.CC_OS_INCR_PER_MONTHS_50_L6M
,x.CC_OS_INCR_PER_MONTHS_50_L12M = y.CC_OS_INCR_PER_MONTHS_50_L12M
,x.CC_OS_INCR_PER_MONTHS_75_L3M = y.CC_OS_INCR_PER_MONTHS_75_L3M
,x.CC_OS_INCR_PER_MONTHS_75_L6M = y.CC_OS_INCR_PER_MONTHS_75_L6M
,x.CC_OS_INCR_PER_MONTHS_75_L12M = y.CC_OS_INCR_PER_MONTHS_75_L12M
,x.CC_OS_INCR_PER_MONTHS_100_L3M = y.CC_OS_INCR_PER_MONTHS_100_L3M
,x.CC_OS_INCR_PER_MONTHS_100_L6M = y.CC_OS_INCR_PER_MONTHS_100_L6M
,x.CC_OS_INCR_PER_MONTHS_100_L12M = y.CC_OS_INCR_PER_MONTHS_100_L12M
,x.CC_OS_DECR_PER_MONTHS_25_L3M = y.CC_OS_DECR_PER_MONTHS_25_L3M
,x.CC_OS_DECR_PER_MONTHS_25_L6M = y.CC_OS_DECR_PER_MONTHS_25_L6M
,x.CC_OS_DECR_PER_MONTHS_25_L12M = y.CC_OS_DECR_PER_MONTHS_25_L12M
,x.CC_OS_DECR_PER_MONTHS_50_L3M = y.CC_OS_DECR_PER_MONTHS_50_L3M
,x.CC_OS_DECR_PER_MONTHS_50_L6M = y.CC_OS_DECR_PER_MONTHS_50_L6M
,x.CC_OS_DECR_PER_MONTHS_50_L12M = y.CC_OS_DECR_PER_MONTHS_50_L12M
,x.CC_OS_DECR_PER_MONTHS_75_L3M = y.CC_OS_DECR_PER_MONTHS_75_L3M
,x.CC_OS_DECR_PER_MONTHS_75_L6M = y.CC_OS_DECR_PER_MONTHS_75_L6M
,x.CC_OS_DECR_PER_MONTHS_75_L12M = y.CC_OS_DECR_PER_MONTHS_75_L12M
,x.CC_OS_DECR_PER_MONTHS_100_L3M = y.CC_OS_DECR_PER_MONTHS_100_L3M
,x.CC_OS_DECR_PER_MONTHS_100_L6M = y.CC_OS_DECR_PER_MONTHS_100_L6M
,x.CC_OS_DECR_PER_MONTHS_100_L12M = y.CC_OS_DECR_PER_MONTHS_100_L12M
FROM SESSION.tmp_grp12 y
WHERE x.RPT_DT = y.RPT_DT
	AND x.MSPHIEU = y.MSPHIEU
    AND x.MA_CIC = y.MA_CIC;

-- CLEAR TMP
    DROP TABLE IF EXISTS SESSION.tmp_filter_date;
	DROP TABLE IF EXISTS SESSION.tmp_CIC_RB06_DUNO_12M;
	DROP TABLE IF EXISTS SESSION.tmp_rn1;
	DROP TABLE IF EXISTS SESSION.tmp_grp12;

end