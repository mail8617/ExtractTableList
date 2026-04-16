/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'TRUNCATE TABLE FE_MK_ONLN_PRMTN_CHNG_TEMP';





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

/* 변경적재 모수 */
INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FE_MK_ONLN_PRMTN_CHNG_TEMP T
SELECT DISTINCT T1.STD_DT
     , T1.PRMTNCD
     , T1.INTG_MEMB_NO
     , T2.PRMTN_STRT_DT
     , T2.PRMTN_END_DT
     , SYSDATE     AS LOAD_DTTM
  FROM
     (
        SELECT /*+ PARALLEL(4) */
               STD_DT
             , PRMTNCD
             , NVL(INTG_MEMB_NO, ONLN_MEMB_NO) INTG_MEMB_NO 
          FROM FE_MK_PRMTN_ORD_TEMP  /* FE_MK_행사주문임시 */
     ) T1
 INNER JOIN D_PRMTN T2
    ON T1.PRMTNCD = T2.PRMTNCD
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_ANALYZE_01  ***************************************************/


DBMS_STATS.GATHER_TABLE_STATS('LDF_DW', 'FE_MK_ONLN_PRMTN_CHNG_TEMP', CASCADE=>TRUE, METHOD_OPT=>'FOR ALL INDEXED COLUMNS', DEGREE=>4);