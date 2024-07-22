--2. Bảng CSO.CIC_RB06_DUNO_VAMC
-- Map thêm TONG_DU_NO
DROP TABLE IF EXISTS rb_vamc;
CREATE TEMPORARY TABLE rb_vamc AS
WITH t1 AS (
    SELECT DISTINCT 
        a.*,
        (SELECT MID_RATE 
         FROM CSO.FCC_CYTB_RATES_HISTORY_OFFICIAL 
         WHERE CCY1 = 'USD' 
         AND RATE_DATE <= CAST(a.RPT_DT AS DATE)
         ORDER BY RATE_DATE DESC 
         FETCH FIRST 1 ROWS ONLY) AS MID_RATE
    FROM 
        CSO.CIC_RB06_DUNO_VAMC a
)
SELECT 
    t1.*,
    COALESCE(CAST(b.TONG_VND AS DECIMAL(18, 2)), 0.0) + COALESCE(CAST(b.TONG_USD AS DECIMAL(18, 2)), 0.0) * t1.MID_RATE AS TONG_DU_NO
FROM 
    t1
LEFT JOIN 
    CSO.CIC_RB06_CTNV b 
ON 
    t1.RPT_DT = b.RPT_DT 
    AND t1.MA_CIC = b.MA_CIC 
    AND t1.MA_TCTD = b.MA_TCTD 
    AND t1.MSPHIEU = b.MSPHIEU;
-- Tính count, tỷ lệ
SELECT 
    MSPHIEU,
    MA_CIC,
    TO_DATE(NGAY_BAOCAO, 'YYYYMMDD') AS NGAY_BAOCAO,
    COALESCE(CAST(NOGOC_CONLAI AS DECIMAL(18, 2)), 0.0) AS NOGOC_CONLAI,
    COUNT(*) OVER (PARTITION BY RPT_DT, MA_CIC, MA_TCTD) AS VAMC_TCTD_CNT,
    SUM(NOGOC_CONLAI) OVER () AS VAMC_OS,
    CASE 
        WHEN TONG_DU_NO <> 0 THEN 
            NOGOC_CONLAI / NULLIF(TONG_DU_NO, 0)
        ELSE 
            NULL
    END AS VAMC_TO_CURRENT_OS_RATIO
FROM 
    rb_vamc;

