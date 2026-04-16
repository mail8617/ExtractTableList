/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FL_MK_PRMTN_EXCH_SL T1
 WHERE EXISTS (
        SELECT 1
          FROM ( SELECT DISTINCT STR_CD
                      , PRMTNCD
                      , INTG_MEMB_NO
                   FROM FL_MK_PRMTN_EXCH_CHNG_TEMP
               ) T2
         WHERE T1.STR_CD       = T2.STR_CD
           AND T1.PRMTNCD      = T2.PRMTNCD
           AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
      );

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_CREATE_01  ****************************************************/


/* 할인행사 데이터 적재 */
EXEC PR_CHK_DROP_TABLE('TEMP_MK_DC');

CREATE TABLE TEMP_MK_DC NOLOGGING AS
SELECT /*+ USE_HASH(T1 T4) PARALLEL(8) */
       T1.STR_CD
     , T1.SL_DT  AS STD_DT
     , T4.INTG_CUST_DISTING_NO
     , T4.INTG_MEMB_NO
     , T1.PRMTNCD
     , T1.EXCH_NO
     , T1.LGPRCD
     , T1.MDPRCD
     , T1.PRMTN_LGCSF_CD
     , T1.PRMTN_MDCSF_CD
     , T1.CMPN_OFFER_NO
     , T1.PRMTN_SECTRG_NO
     , T1.PRMTN_APLY_STRT_AMT
     , SUM(T4.DLR_TOT_NSALAMT*T4.SALES_SIGN) OVER (PARTITION BY T4.ORIG_EXCH_NO) AS RETR
FROM (
       SELECT /*+ PARALLEL(4) */
             DISTINCT
              T3.STR_CD
            , T3.SL_DT
            , T3.INTG_MEMB_NO
            , T3.PRMTNCD
            , T3.EXCH_NO
            , T2.LGPRCD
            , T2.MDPRCD
            , T2.PRMTN_LGCSF_CD
            , T2.PRMTN_MDCSF_CD
            , T5.CMPN_OFFER_NO
            , T5.PRMTN_SECTRG_NO
            , T5.PRMTN_APLY_STRT_AMT
            , T2.PRMTN_STRT_DT
            , T2.PRMTN_END_DT
         FROM FL_SL_PROD_DC T3 /* FL_SL_상품할인 */
        INNER JOIN
            (  /* 변경적재 */
               SELECT DISTINCT STR_CD
                    , PRMTNCD
                    , INTG_MEMB_NO
                 FROM FL_MK_PRMTN_EXCH_CHNG_TEMP
            ) T1
           ON T1.STR_CD           = T3.STR_CD
          AND T1.PRMTNCD          = T3.PRMTNCD
          AND T1.INTG_MEMB_NO     = T3.INTG_MEMB_NO
        INNER JOIN D_PRMTN T2  /* D_행사 */
           ON T3.PRMTNCD          = T2.PRMTNCD
        INNER JOIN WL_SL_SALE_HDR_DC T31 /* WL_SL_판매할인헤더 */
           ON T3.SL_DT            = T31.SL_DT
          AND T3.STR_CD           = T31.STR_CD
          AND T3.EXCH_NO          = T31.EXCH_NO
          AND T3.EXCH_NO_DC_SEQ   = T31.EXCH_NO_DC_SEQ
         LEFT OUTER JOIN WL_LC_PRMTN_OFFER_SECT T5  /* WL_LC_행사제공구간 */
           ON T31.CMPN_OFFER_NO   = T5.CMPN_OFFER_NO
          AND T31.PRMTN_SECTRG_NO = T5.PRMTN_SECTRG_NO
          AND T5.USE_YN           = '1'
        WHERE T2.PRMTN_LGCSF_CD   = '001'    /* 할인 */
     ) T1
 INNER JOIN FL_SL_EXCH_SL T4 /* FL_SL_교환권판매 */
    ON T1.SL_DT         = T4.SL_DT
   AND T1.STR_CD        = T4.STR_CD
   AND T1.EXCH_NO       = T4.EXCH_NO
   AND T4.SL_DT   BETWEEN T1.PRMTN_STRT_DT AND T1.PRMTN_END_DT
;





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FL_MK_PRMTN_EXCH_SL T
SELECT /*+ PARALLEL(8) */
       T1.PRMTNCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.EXCH_NO
     , CASE WHEN T3.EXCH_NO IS NOT NULL THEN 'Y' ELSE 'N' END   AS EXCH_PRMTN_APLY_YN
     , T1.SALES_SIGN
     , T1.INTG_MEMB_NO
     , T1.INTG_CUST_DISTING_NO
     , T1.CUST_DISTING_NO
     , T1.NATLT_CD
     , T1.GRP_NO
     , T1.GRP_TYPE_CD
     , T1.CUST_CLSF_CD
     , T1.GRP_CLSF_CD
     , T1.CUST_SALES_DVS_CD
     , T1.SALES_HAPN_LOCTN_CD
     , T1.SL_CHNL_CD
     , T1.SL_MEDIA_DVS_CD
     , T1.ONLN_SL_DVS_CD
     , T1.IMGN_INFO_DVS_CD   /* 출입국정보구분코드 */
     , T1.IMGN_DT            /* 출입국일자 */
     , T1.LGPRCD
     , T1.MDPRCD
     , T1.PRMTN_LGCSF_CD
     , T1.PRMTN_MDCSF_CD
     , T3.CMPN_OFFER_NO
     , T3.PRMTN_SECTRG_NO
     , T3.PRMTN_APLY_STRT_AMT
     , T1.DLR_TOT_NSALAMT
     , T1.WON_TOT_NSALAMT
     , T1.DLR_TOT_DC_AMT
     , T1.WON_TOT_DC_AMT
     , 0                       AS DLR_ARSE_NSALAMT
     , 0                       AS WON_ARSE_NSALAMT
     , SYSDATE                 AS LOAD_DTTM
  FROM (
        SELECT /*+ PARALLEL(4) */
               T2.PRMTNCD
             , T2.STR_CD
             , T2.STD_DT
             , T1.EXCH_NO
             , T1.SALES_SIGN
             , T1.INTG_CUST_DISTING_NO
             , T2.INTG_MEMB_NO
             , T1.CUST_DISTING_NO
             , T1.NATLT_CD
             , T1.GRP_NO
             , T1.GRP_TYPE_CD
             , T1.CUST_CLSF_CD
             , T1.GRP_CLSF_CD
             , T1.CUST_SALES_DVS_CD
             , T1.SALES_HAPN_LOCTN_CD
             , T1.SL_CHNL_CD
             , T1.SL_MEDIA_DVS_CD
             , T1.ONLN_SL_DVS_CD
             , T1.IMGN_INFO_DVS_CD   /*출입국정보구분코드*/
             , T1.IMGN_DT            /*출입국일자 */
             , T2.LGPRCD
             , T2.MDPRCD
             , T2.PRMTN_LGCSF_CD
             , T2.PRMTN_MDCSF_CD
             , T1.DLR_TOT_NSALAMT
             , T1.WON_TOT_NSALAMT
             , T1.DLR_TOT_DC_AMT
             , T1.WON_TOT_DC_AMT
          FROM ( /* 당일당점 매출 */
                SELECT /*+ PARALLEL(4) */
                       STR_CD
                     , STD_DT
                     , INTG_MEMB_NO
                     , PRMTNCD
                     , MAX(LGPRCD        ) AS LGPRCD
                     , MAX(MDPRCD        ) AS MDPRCD
                     , MAX(PRMTN_LGCSF_CD) AS PRMTN_LGCSF_CD
                     , MAX(PRMTN_MDCSF_CD) AS PRMTN_MDCSF_CD
                  FROM TEMP_MK_DC
                 WHERE RETR > 0  /* 취소된 건들은 모두 제외 */
                 GROUP BY STR_CD
                        , STD_DT
                        , INTG_MEMB_NO
                        , PRMTNCD
               ) T2
         INNER JOIN FL_SL_EXCH_SL T1 /* FL_SL_교환권판매 */
            ON T2.STR_CD       = T1.STR_CD
           AND T2.STD_DT       = T1.SL_DT
           AND T2.INTG_MEMB_NO = T1.INTG_MEMB_NO
        ) T1
  LEFT OUTER JOIN (
       SELECT /*+ PARALLEL(4) */
              A2.STD_DT
            , A2.STR_CD
            , A2.EXCH_NO
            , A2.PRMTNCD
            , MAX(A2.CMPN_OFFER_NO      ) AS CMPN_OFFER_NO
            , MAX(A2.PRMTN_SECTRG_NO    ) AS PRMTN_SECTRG_NO
            , MAX(A2.PRMTN_APLY_STRT_AMT) AS PRMTN_APLY_STRT_AMT
         FROM TEMP_MK_DC A2
        WHERE A2.RETR > 0
        GROUP BY A2.STD_DT
               , A2.STR_CD
               , A2.EXCH_NO
               , A2.PRMTNCD
       ) T3
    ON T1.PRMTNCD = T3.PRMTNCD /* 교환권매출 교환권 */
   AND T1.STD_DT  = T3.STD_DT
   AND T1.STR_CD  = T3.STR_CD
   AND T1.EXCH_NO = T3.EXCH_NO
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_CREATE_02  ****************************************************/


/* 구매증정 데이터 적재 */
EXEC PR_CHK_DROP_TABLE('TEMP_MK_PRESTAT');

CREATE TABLE TEMP_MK_PRESTAT NOLOGGING AS
SELECT /*+ USE_HASH(T1 T5) PARALLEL(8) */
       DISTINCT T3.STR_CD
     , T4.SL_DT   AS STD_DT
     , NVL(T4.INTG_MEMB_NO,T1.INTG_MEMB_NO) AS INTG_CUST_DISTING_NO
     , NVL(T4.INTG_MEMB_NO,T1.INTG_MEMB_NO) AS INTG_MEMB_NO
     , T3.PRMTNCD
     , T4.EXCH_NO
     , T2.LGPRCD
     , T2.MDPRCD
     , T2.PRMTN_LGCSF_CD
     , T2.PRMTN_MDCSF_CD
     , T5.CMPN_OFFER_NO
     , T5.PRMTN_SECTRG_NO
     , T5.PRMTN_APLY_STRT_AMT
     , SUM(DECODE(T5.PRESTAT_DVS_CD, '0', 1, -1)) OVER(PARTITION BY T3.PRMTNCD, T3.STR_CD, CASE WHEN T5.PRESTAT_DVS_CD = '0' THEN T5.PRESTAT_SN  ELSE T5.ORIG_PRESTAT_SN END) RETR
  FROM (
        SELECT DISTINCT STR_CD
             , PRMTNCD
             , INTG_MEMB_NO
          FROM FL_MK_PRMTN_EXCH_CHNG_TEMP
      ) T1
 INNER JOIN WL_MK_PRMTN_FGF_PRESTAT T5 /* WL_MK_행사사은품증정 */
    ON T1.STR_CD         = T5.STR_CD
   AND T1.PRMTNCD        = T5.PRMTNCD
   AND T1.INTG_MEMB_NO   = T5.INTG_MEMB_NO
 INNER JOIN WL_MK_PRMTN_FGF_PRESTAT_EXCH T3 /* WL_MK_행사사은품증정교환권 */
    ON T3.STR_CD         = T5.STR_CD
   AND T3.PRMTNCD        = T5.PRMTNCD
   AND T3.PRESTAT_DVS_CD = T5.PRESTAT_DVS_CD
   AND T3.PRESTAT_SN     = T5.PRESTAT_SN
 INNER JOIN D_PRMTN T2 /* D_행사 */
    ON T3.PRMTNCD  = T2.PRMTNCD
 INNER JOIN FL_SL_EXCH_SL T4  /* FL_SL_교환권판매 */
    ON T3.SALES_DT = T4.SL_DT
   AND T3.STR_CD   = T4.STR_CD
   AND T3.EXCH_NO  = T4.EXCH_NO
   AND T1.INTG_MEMB_NO = T4.INTG_MEMB_NO    /*20231108추가*/
 WHERE T4.SL_DT BETWEEN T2.PRMTN_STRT_DT AND T2.PRMTN_END_DT
   AND T2.PRMTN_LGCSF_CD IN ('003','005')  /* 003 사은품 005 LDFPAY */
   AND NVL(T2.PCHS_DVS_CD,'z') <> '02' /* 비구매행사제외 */
;





/**********  UQ_INSERT_02  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FL_MK_PRMTN_EXCH_SL T
SELECT /*+ PARALLEL(8) */
       T1.PRMTNCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.EXCH_NO
     , CASE WHEN T3.EXCH_NO IS NOT NULL THEN 'Y' ELSE 'N' END   AS EXCH_PRMTN_APLY_YN
     , T1.SALES_SIGN
     , T1.INTG_MEMB_NO
     , T1.INTG_CUST_DISTING_NO
     , T1.CUST_DISTING_NO
     , T1.NATLT_CD
     , T1.GRP_NO
     , T1.GRP_TYPE_CD
     , T1.CUST_CLSF_CD
     , T1.GRP_CLSF_CD
     , T1.CUST_SALES_DVS_CD
     , T1.SALES_HAPN_LOCTN_CD
     , T1.SL_CHNL_CD
     , T1.SL_MEDIA_DVS_CD
     , T1.ONLN_SL_DVS_CD
     , T1.IMGN_INFO_DVS_CD   /*출입국정보구분코드*/
     , T1.IMGN_DT            /*출입국일자 */
     , T1.LGPRCD
     , T1.MDPRCD
     , T1.PRMTN_LGCSF_CD
     , T1.PRMTN_MDCSF_CD
     , T3.CMPN_OFFER_NO
     , T3.PRMTN_SECTRG_NO
     , T3.PRMTN_APLY_STRT_AMT
     , T1.DLR_TOT_NSALAMT
     , T1.WON_TOT_NSALAMT
     , T1.DLR_TOT_DC_AMT
     , T1.WON_TOT_DC_AMT
     , 0       AS DLR_ARSE_NSALAMT
     , 0       AS WON_ARSE_NSALAMT
     , SYSDATE AS LOAD_DTTM
  FROM (
        SELECT /*+ PARALLEL(4) */
               T2.PRMTNCD
             , T2.STR_CD
             , T2.STD_DT
             , T1.EXCH_NO
             , T1.SALES_SIGN
             , T1.INTG_CUST_DISTING_NO
             , T2.INTG_MEMB_NO
             , T1.CUST_DISTING_NO
             , T1.NATLT_CD
             , T1.GRP_NO
             , T1.GRP_TYPE_CD
             , T1.CUST_CLSF_CD
             , T1.GRP_CLSF_CD
             , T1.CUST_SALES_DVS_CD
             , T1.SALES_HAPN_LOCTN_CD
             , T1.SL_CHNL_CD
             , T1.SL_MEDIA_DVS_CD
             , T1.ONLN_SL_DVS_CD
             , T1.IMGN_INFO_DVS_CD   /*출입국정보구분코드*/
             , T1.IMGN_DT            /*출입국일자 */
             , T2.LGPRCD
             , T2.MDPRCD
             , T2.PRMTN_LGCSF_CD
             , T2.PRMTN_MDCSF_CD
             , T1.DLR_TOT_NSALAMT
             , T1.WON_TOT_NSALAMT
             , T1.DLR_TOT_DC_AMT
             , T1.WON_TOT_DC_AMT
          FROM ( /* 당일당점 매출 */
                SELECT /*+ PARALLEL(4) */
                       STR_CD
                     , STD_DT
                     , INTG_MEMB_NO
                     , PRMTNCD
                     , MAX(LGPRCD        ) AS LGPRCD
                     , MAX(MDPRCD        ) AS MDPRCD
                     , MAX(PRMTN_LGCSF_CD) AS PRMTN_LGCSF_CD
                     , MAX(PRMTN_MDCSF_CD) AS PRMTN_MDCSF_CD
                  FROM TEMP_MK_PRESTAT
                 WHERE RETR > 0  /* 취소된 건들은 모두 제외 */
                 GROUP BY STR_CD
                        , STD_DT
                        , INTG_MEMB_NO
                        , PRMTNCD
               ) T2
         INNER JOIN FL_SL_EXCH_SL T1  /* FL_SL_교환권판매 */
            ON T2.STR_CD       = T1.STR_CD
           AND T2.STD_DT       = T1.SL_DT
           AND T2.INTG_MEMB_NO = T1.INTG_MEMB_NO
       ) T1
  LEFT OUTER JOIN (
       SELECT /*+ PARALLEL(4) */
              A2.STD_DT
            , A2.STR_CD
            , A2.EXCH_NO
            , A2.PRMTNCD
            , MAX(A2.CMPN_OFFER_NO      ) AS CMPN_OFFER_NO
            , MAX(A2.PRMTN_SECTRG_NO    ) AS PRMTN_SECTRG_NO
            , MAX(A2.PRMTN_APLY_STRT_AMT) AS PRMTN_APLY_STRT_AMT
         FROM TEMP_MK_PRESTAT A2
        WHERE A2.RETR > 0
        GROUP BY A2.STD_DT
               , A2.STR_CD
               , A2.EXCH_NO
               , A2.PRMTNCD
       ) T3
    ON T1.PRMTNCD = T3.PRMTNCD /* -교환권매출 교환권 */
   AND T1.STD_DT  = T3.STD_DT
   AND T1.STR_CD  = T3.STR_CD
   AND T1.EXCH_NO = T3.EXCH_NO
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_03  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FL_MK_PRMTN_EXCH_SL T
SELECT /*+ USE_HASH(T3 T1) PARALLEL(16) */
       T3.PRMTNCD
     , T3.STR_CD
     , T3.STD_DT
     , NVL(T1.EXCH_NO,'z') AS EXCH_NO
     , 'N'                 AS EXCH_PRMTN_APLY_YN
     , MAX(T1.SALES_SIGN)  AS SALES_SIGN
     , T3.INTG_MEMB_NO
     , MAX(NVL(T3.INTG_MEMB_NO,INTG_CUST_DISTING_NO) ) AS INTG_CUST_DISTING_NO
     , MAX(NVL(T3.CUST_DISTING_NO,T1.CUST_DISTING_NO)) AS CUST_DISTING_NO
     , NVL(MAX(T2.NATLT_CD),'z') AS NATLT_CD       
     , NVL(MAX(T1.GRP_NO             ),'z') AS GRP_NO   
     , NVL(MAX(T1.GRP_TYPE_CD        ),'z') AS GRP_TYPE_CD
     , NVL(MAX(T1.CUST_CLSF_CD       ),'z') AS CUST_CLSF_CD
     , NVL(MAX(T1.GRP_CLSF_CD        ),'z') AS GRP_CLSF_CD
     , MAX(NVL(T1.CUST_SALES_DVS_CD  ,T3.CUST_SALES_DVS_CD)) AS CUST_SALES_DVS_CD
     , NVL(MAX(T1.SALES_HAPN_LOCTN_CD),'z') AS SALES_HAPN_LOCTN_CD
     , NVL(MAX(T1.SL_CHNL_CD         ),'z') AS SL_CHNL_CD
     , NVL(MAX(T1.SL_MEDIA_DVS_CD    ),'z') AS SL_MEDIA_DVS_CD
     , NVL(MAX(T1.ONLN_SL_DVS_CD     ),'z') AS ONLN_SL_DVS_CD
     , NVL(MAX(T1.IMGN_INFO_DVS_CD   ),'z') AS IMGN_INFO_DVS_CD   /*출입국정보구분코드*/
     , MAX(T1.IMGN_DT)            /*출입국일자 */
     , NVL(MAX(T3.LGPRCD             ),'z') AS LGPRCD        
     , NVL(MAX(T3.MDPRCD             ),'z') AS MDPRCD        
     , NVL(MAX(T3.PRMTN_LGCSF_CD     ),'z') AS PRMTN_LGCSF_CD
     , NVL(MAX(T3.PRMTN_MDCSF_CD     ),'z') AS PRMTN_MDCSF_CD
     , MAX(T3.CMPN_OFFER_NO      )          AS CMPN_OFFER_NO      
     , MAX(T3.PRMTN_SECTRG_NO    )          AS PRMTN_SECTRG_NO    
     , MAX(T3.PRMTN_APLY_STRT_AMT)          AS PRMTN_APLY_STRT_AMT
     , SUM(NVL(T1.DLR_TOT_NSALAMT,0))       AS DLR_TOT_NSALAMT
     , SUM(NVL(T1.WON_TOT_NSALAMT,0))       AS WON_TOT_NSALAMT
     , SUM(NVL(T1.DLR_TOT_DC_AMT ,0))       AS DLR_TOT_DC_AMT
     , SUM(NVL(T1.WON_TOT_DC_AMT ,0))       AS WON_TOT_DC_AMT
     , 0  AS DLR_ARSE_NSALAMT
     , 0  AS WON_ARSE_NSALAMT
     , SYSDATE LOAD_DTTM
  FROM (
        SELECT /*+ PARALLEL(4) */
               DISTINCT T3.STR_CD
             , T3.PYMT_STD_DT   AS STD_DT
             , T3.INTG_MEMB_NO
             , T3.PRMTNCD
             , CASE WHEN T3.VIP_NO IS NOT NULL THEN '1' ELSE '2' END  AS CUST_SALES_DVS_CD
             , NVL(T3.VIP_NO, T3.PRMTN_NATLT_CD||T3.PSPT_RCGNT_NO) AS CUST_DISTING_NO
             , T3.CMPN_OFFER_NO
             , T3.PRMTN_SECTRG_NO
             , T3.PRMTN_APLY_STRT_AMT
             , T2.LGPRCD
             , T2.MDPRCD
             , T2.PRMTN_LGCSF_CD
             , T2.PRMTN_MDCSF_CD
             , SUM(DECODE(T3.PRESTAT_DVS_CD, '0', 1, -1)) OVER(PARTITION BY T3.PRMTNCD, T3.STR_CD, CASE WHEN T3.PRESTAT_DVS_CD = '0' THEN T3.PRESTAT_SN  ELSE T3.ORIG_PRESTAT_SN END) RETR
          FROM (
                SELECT DISTINCT STR_CD
                     , PRMTNCD
                     , INTG_MEMB_NO
                  FROM FL_MK_PRMTN_EXCH_CHNG_TEMP
              ) T1
         INNER JOIN WL_MK_PRMTN_FGF_PRESTAT T3  /* WL_MK_행사사은품증정 */
            ON T1.STR_CD       = T3.STR_CD
           AND T1.PRMTNCD      = T3.PRMTNCD
           AND T1.INTG_MEMB_NO = T3.INTG_MEMB_NO
         INNER JOIN D_PRMTN T2 /* D_행사 */
            ON T3.PRMTNCD       = T2.PRMTNCD
           AND T3.PRESTAT_DT BETWEEN T2.PRMTN_STRT_DT AND T2.PRMTN_END_DT
           AND T2.PRMTN_LGCSF_CD IN ('003','005')  /* 003 사은품 005 LDFPAY */
           AND NVL(T2.PCHS_DVS_CD,'z') = '02' /* 비구매 */
      ) T3
 INNER JOIN D_INTG_MEMB T2
    ON T3.INTG_MEMB_NO = T2.INTG_MEMB_NO
  LEFT OUTER JOIN FL_SL_EXCH_SL T1 /* FL_SL_교환권판매 */
    ON T3.STD_DT       = T1.SL_DT
   AND T3.STR_CD       = T1.STR_CD
   AND T3.INTG_MEMB_NO = T1.INTG_MEMB_NO
 WHERE T3.RETR > 0
 GROUP BY T3.PRMTNCD
     , T3.STR_CD
     , T3.STD_DT
     , NVL(T1.EXCH_NO,'z') 
     , T3.INTG_MEMB_NO
 ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_MERGE_01  *****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

/* LDFPAY유발매출 */
MERGE /*+ APPEND PARALLEL(4) */
 INTO FL_MK_PRMTN_EXCH_SL T1
USING (
       SELECT /*+ PARALLEL(4) */
              T1.PRMTNCD
            , T1.STR_CD
            , T1.STD_DT
            , T1.EXCH_NO
            , 'N'           AS EXCH_PRMTN_APLY_YN
            , T2.SALES_SIGN
            , T1.INTG_MEMB_NO
            , T2.INTG_CUST_DISTING_NO
            , T2.CUST_DISTING_NO
            , T2.NATLT_CD
            , T2.GRP_NO
            , T2.GRP_TYPE_CD
            , T2.CUST_CLSF_CD
            , T2.GRP_CLSF_CD
            , T2.CUST_SALES_DVS_CD
            , T2.SALES_HAPN_LOCTN_CD
            , T2.SL_CHNL_CD
            , T2.SL_MEDIA_DVS_CD
            , T2.ONLN_SL_DVS_CD
            , T2.IMGN_INFO_DVS_CD   /*출입국정보구분코드*/
            , T2.IMGN_DT            /*출입국일자 */
            , T1.LGPRCD
            , T1.MDPRCD
            , T1.PRMTN_LGCSF_CD
            , T1.PRMTN_MDCSF_CD
            , T1.CMPN_OFFER_NO    AS CMPN_OFFER_NO
            , T1.PRMTN_SECTRG_NO  AS PRMTN_SECTRG_NO
            , NULL                AS PRMTN_APLY_STRT_AMT
            , 0                   AS DLR_TOT_NSALAMT
            , 0                   AS WON_TOT_NSALAMT
            , 0                   AS DLR_TOT_DC_AMT
            , 0                   AS WON_TOT_DC_AMT
            , T2.DLR_TOT_NSALAMT  AS DLR_ARSE_NSALAMT      /* 달러유발순매출액        */
            , T2.WON_TOT_NSALAMT  AS WON_ARSE_NSALAMT      /* 원화유발순매출액        */
            , SYSDATE             AS LOAD_DTTM
         FROM (
              SELECT PRMTNCD
                   , STR_CD
                   , STD_DT
                   , EXCH_NO
                   , INTG_MEMB_NO
                   , LGPRCD
                   , MDPRCD
                   , PRMTN_LGCSF_CD
                   , PRMTN_MDCSF_CD
                   , MAX(CMPN_OFFER_NO)    CMPN_OFFER_NO
                   , MAX(PRMTN_SECTRG_NO)  PRMTN_SECTRG_NO
                   /* 유발매출의 경우 한교환권에 여러 ldfpay의 증정 구간대가 존재해서 하나의 구간대에 유발매출을 부여해줘야 
                      유발매출금액이 중복되지 않아 MAX처리함 */
                FROM (
                     SELECT  /*+ PARALLEL(4) */
                             T1.PRMTNCD
                           , T1.STR_CD
                           , T1.LDFP_HIST_HAPN_DT   AS STD_DT
                           , T1.EXCH_NO
                           , T1.INTG_MEMB_NO
                           , T3.LGPRCD
                           , T3.MDPRCD
                           , T3.PRMTN_LGCSF_CD
                           , T3.PRMTN_MDCSF_CD
                           , T1.CMPN_OFFER_NO   
                           , T1.PRMTN_SECTRG_NO  
                           , SUM(CASE WHEN T1.LDFP_RCPDSBS_TYPE_CD in('21','26') THEN T1.LDFP_USE_AMT ELSE 0 END) OVER(PARTITION BY T1.PRMTNCD, T1.STR_CD, T1.LDFP_HIST_HAPN_DT, T1.INTG_MEMB_NO, T1.CMPN_OFFER_NO, T1.PRMTN_SECTRG_NO )  AS RETR
                        FROM FL_MK_LDFP_ACMLT_USE_PTCLS  T1 /* LDFPAY적립사용내역 */
                       INNER JOIN (
                              SELECT DISTINCT STR_CD
                                   , PRMTNCD
                                   , INTG_MEMB_NO
                                FROM FL_MK_PRMTN_EXCH_CHNG_TEMP
                           ) T2
                          ON T1.STR_CD       = T2.STR_CD
                         AND T1.PRMTNCD      = T2.PRMTNCD
                         AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
                       INNER JOIN D_PRMTN T3 /* D_행사 */
                          ON T1.PRMTNCD        = T3.PRMTNCD
                         AND T3.PRMTN_LGCSF_CD = '005'  /* LDFPAY */
                         AND T1.LDFP_HIST_HAPN_DT BETWEEN T3.PRMTN_STRT_DT AND (CASE WHEN T3.PRMTN_END_DT < '99991201'
                                                                                     THEN TO_CHAR(TO_DATE(T3.PRMTN_END_DT,'YYYYMMDD') + 30,'YYYYMMDD')
                                                                                     ELSE T3.PRMTN_END_DT END)
                       WHERE T1.LDFP_ACTI_YN     = 'Y'
                         AND T1.LDFP_NO_STAT_CD <> '00'
                         AND T1.ACMLT_USE_DVS_CD = '2'  /* 사용 */
                         AND T1.LDFP_RCPDSBS_TYPE_CD in('21','26')
                      ) T1
                 WHERE RETR <> 0
                 GROUP BY   
                     PRMTNCD
                   , STR_CD
                   , STD_DT
                   , EXCH_NO
                   , INTG_MEMB_NO
                   , LGPRCD
                   , MDPRCD
                   , PRMTN_LGCSF_CD
                   , PRMTN_MDCSF_CD
              ) T1
         INNER JOIN FL_SL_EXCH_SL T2  /* FL_SL_교환권판매 */
            ON T1.STD_DT  = T2.SL_DT
           AND T1.STR_CD  = T2.STR_CD
           AND T1.EXCH_NO = T2.EXCH_NO
      ) T2
ON (
        T1.PRMTNCD   = T2.PRMTNCD
    AND T1.STR_CD    = T2.STR_CD
    AND T1.STD_DT    = T2.STD_DT
    AND T1.EXCH_NO   = T2.EXCH_NO
    AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
   )
WHEN MATCHED THEN
 UPDATE SET T1.DLR_ARSE_NSALAMT = T2.DLR_ARSE_NSALAMT /* 달러유발순매출액 */
          , T1.WON_ARSE_NSALAMT = T2.WON_ARSE_NSALAMT /* 원화유발순매출액 */
          , T1.LOAD_DTTM        = SYSDATE
WHEN NOT MATCHED THEN
 INSERT (
          PRMTNCD                 /* 행사코드           */
        , STR_CD                  /* 점코드             */
        , STD_DT                  /* 기준일자           */
        , EXCH_NO                 /* 교환권번호         */
        , EXCH_PRMTN_APLY_YN      /* 교환권행사적용여부 */
        , SALES_SIGN              /* 매출부호           */
        , INTG_MEMB_NO            /* 통합회원번호       */
        , INTG_CUST_DISTING_NO    /* 통합고객식별번호   */
        , CUST_DISTING_NO         /* 고객식별번호       */
        , NATLT_CD                /* 국적코드           */
        , GRP_NO                  /* 단체번호           */
        , GRP_TYPE_CD             /* 단체유형코드       */
        , CUST_CLSF_CD            /* 고객분류코드       */
        , GRP_CLSF_CD             /* 단체분류코드       */
        , CUST_SALES_DVS_CD       /* 고객매출구분코드   */
        , SALES_HAPN_LOCTN_CD     /* 매출발생위치코드   */
        , SL_CHNL_CD              /* 판매채널코드       */
        , SL_MEDIA_DVS_CD         /* 판매매체구분코드   */
        , ONLN_SL_DVS_CD          /* 온라인판매구분코드 */
        , IMGN_INFO_DVS_CD        /* 출입국정보구분코드 */
        , IMGN_DT                 /* 출입국일자         */
        , LGPRCD                  /* 대행사코드         */
        , MDPRCD                  /* 중행사코드         */
        , PRMTN_LGCSF_CD          /* 행사대분류코드     */
        , PRMTN_MDCSF_CD          /* 행사중분류코드     */
        , CMPN_OFFER_NO           /* 캠페인OFFER번호    */
        , PRMTN_SECTRG_NO         /* 행사구간대번호     */
        , PRMTN_APLY_STRT_AMT     /* 행사적용시작금액   */
        , DLR_TOT_NSALAMT         /* 달러총순매출액     */
        , WON_TOT_NSALAMT         /* 원화총순매출액     */
        , DLR_TOT_DC_AMT          /* 달러총할인금액     */
        , WON_TOT_DC_AMT          /* 원화총할인금액     */
        , DLR_ARSE_NSALAMT        /* 달러유발순매출액   */
        , WON_ARSE_NSALAMT        /* 원화유발순매출액   */
        , LOAD_DTTM               /* 적재일시           */
 )
 VALUES (
          T2.PRMTNCD
        , T2.STR_CD
        , T2.STD_DT
        , T2.EXCH_NO
        , 'N'
        , T2.SALES_SIGN
        , T2.INTG_MEMB_NO
        , T2.INTG_CUST_DISTING_NO
        , T2.CUST_DISTING_NO
        , T2.NATLT_CD
        , T2.GRP_NO
        , T2.GRP_TYPE_CD
        , T2.CUST_CLSF_CD
        , T2.GRP_CLSF_CD
        , T2.CUST_SALES_DVS_CD
        , T2.SALES_HAPN_LOCTN_CD
        , T2.SL_CHNL_CD
        , T2.SL_MEDIA_DVS_CD
        , T2.ONLN_SL_DVS_CD
        , T2.IMGN_INFO_DVS_CD   /*출입국정보구분코드*/
        , T2.IMGN_DT            /*출입국일자 */
        , T2.LGPRCD
        , T2.MDPRCD
        , T2.PRMTN_LGCSF_CD
        , T2.PRMTN_MDCSF_CD
        , T2.CMPN_OFFER_NO    
        , T2.PRMTN_SECTRG_NO
        , NULL
        , 0
        , 0
        , 0
        , 0
        , T2.DLR_ARSE_NSALAMT
        , T2.WON_ARSE_NSALAMT
        , SYSDATE
) ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;