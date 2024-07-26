/*
 * Author: BinhNN2
 * Database: DB2
 * Project: CIC LONGLIST
 * PRIMARY KEY: RPT_DT, MSPHIEU, MA_CIC
 * PURPOSE: This procedure inserts data into TPBRM1.CIC_FEATURE_STORE_KHCN from CSO.CIC_RB04, CSO.CIC_RB06, and CSO.CIC_RB21
 *          based on the provided start_date and end_date parameters.
 * First date: 2024-07-17
 */
-- DROP PROCEDURE CIC_INIT;

CREATE OR REPLACE PROCEDURE CIC_INIT(
    IN start_date DATE,
    IN end_date DATE
)
LANGUAGE SQL
BEGIN
	DECLARE SQLSTATE CHAR(5);

	MERGE INTO TPBRM1.CIC_FEATURE_STORE_KHCN AS target
	USING (
	    SELECT RPT_DT, MSPHIEU, MA_CIC, 'KHCN' AS CUSTOMER_TYPE FROM CSO.CIC_RB04
	    UNION
	    SELECT RPT_DT, MSPHIEU, MA_CIC, 'KHCN' AS CUSTOMER_TYPE FROM CSO.CIC_RB06
	    UNION
	    SELECT RPT_DT, MSPHIEU, MA_CIC, 'KHCN' AS CUSTOMER_TYPE FROM CSO.CIC_RB21
	) AS source
	ON target.RPT_DT = source.RPT_DT AND target.MSPHIEU = source.MSPHIEU AND target.MA_CIC = source.MA_CIC
	WHEN NOT MATCHED AND MSPHIEU IS NOT NULL AND MA_CIC IS NOT NULL AND TO_DATE(source.RPT_DT, 'YYYYMMDD') BETWEEN start_date AND end_date THEN
	    INSERT (RPT_DT, MSPHIEU, MA_CIC, CUSTOMER_TYPE)
	    VALUES (source.RPT_DT, source.MSPHIEU, source.MA_CIC, source.CUSTOMER_TYPE);

COMMIT;

END;




