CREATE OR REPLACE PROCEDURE CIC_MAIN_PROCEDURE(
    IN start_date DATE,
    IN end_date DATE
)
LANGUAGE SQL
BEGIN
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;

    -- Call and log CIC_INIT
    SET v_start_time = CURRENT TIMESTAMP;
    CALL CIC_INIT(start_date, end_date);
    SET v_end_time = CURRENT TIMESTAMP;
    CALL CIC_LOG_PROCEDURE_RUN('CIC_INIT', v_start_time, v_end_time);

    -- Call and log CIC_RB06_DUNO_12M
    SET v_start_time = CURRENT TIMESTAMP;
    CALL CIC_RB06_DUNO_12M(start_date, end_date);
    SET v_end_time = CURRENT TIMESTAMP;
    CALL CIC_LOG_PROCEDURE_RUN('CIC_RB06_DUNO_12M', v_start_time, v_end_time);

    -- Call and log CIC_RB06_DUNO_THETD
    SET v_start_time = CURRENT TIMESTAMP;
    CALL CIC_RB06_DUNO_THETD(start_date, end_date);
    SET v_end_time = CURRENT TIMESTAMP;
    CALL CIC_LOG_PROCEDURE_RUN('CIC_RB06_DUNO_THETD', v_start_time, v_end_time);
   
       -- Call and log CIC_RB06_DUNO_THETD_LANVH
    SET v_start_time = CURRENT TIMESTAMP;
    CALL CIC_RB06_DUNO_THETD_LANVH(start_date, end_date);
    SET v_end_time = CURRENT TIMESTAMP;
    CALL CIC_LOG_PROCEDURE_RUN('CIC_RB06_DUNO_THETD_LANVH', v_start_time, v_end_time);
   
       -- Call and log CIC_RB06_DUNO_VAMC_LANVH
    SET v_start_time = CURRENT TIMESTAMP;
    CALL CIC_RB06_DUNO_VAMC_LANVH(start_date, end_date);
    SET v_end_time = CURRENT TIMESTAMP;
    CALL CIC_LOG_PROCEDURE_RUN('CIC_RB06_DUNO_VAMC_LANVH', v_start_time, v_end_time);
   
       -- Call and log CIC_RB06_HDTD_LANVH
    SET v_start_time = CURRENT TIMESTAMP;
    CALL CIC_RB06_HDTD_LANVH(start_date, end_date);
    SET v_end_time = CURRENT TIMESTAMP;
    CALL CIC_LOG_PROCEDURE_RUN('CIC_RB06_HDTD_LANVH', v_start_time, v_end_time);
   
   -- Call and log CIC_RB06_CTNV_HUYENDT14
    SET v_start_time = CURRENT TIMESTAMP;
    CALL CIC_RB06_CTNV_HUYENDT14(start_date, end_date);
    SET v_end_time = CURRENT TIMESTAMP;
    CALL CIC_LOG_PROCEDURE_RUN('CIC_RB06_CTNV_HUYENDT14', v_start_time, v_end_time);
   
END;
