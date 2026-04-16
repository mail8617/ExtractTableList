/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FE_MK_PRMTN_ORD_SL T1
 WHERE EXISTS (
                SELECT 1
                  FROM ( SELECT DISTINCT T1.STD_DT
                             , T1.PRMTNCD
                             , T1.INTG_MEMB_NO INTG_MEMB_NO 
                          FROM FE_MK_PRMTN_ORD_TEMP  T1 /* FE_MK_행사주문임시 */
                       ) T2
                 WHERE  T1.PRMTNCD      = T2.PRMTNCD
                   AND  T1.STD_DT       = T2.STD_DT
                   AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
              ) ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_DELETE_01(마스터번호)  ****************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FE_MK_PRMTN_ORD_SL T1
 WHERE EXISTS (
                SELECT 1
                  FROM ( SELECT DISTINCT T1.STD_DT
                             , T1.PRMTNCD
                             , NVL(T1.INTG_MEMB_NO, T2.INTG_MEMB_NO ) INTG_MEMB_NO 
                          FROM FE_MK_PRMTN_ORD_TEMP  T1 /* FE_MK_행사주문임시 */
                          JOIN D_INTG_MEMB T2
                            ON T1.ONLN_MEMB_NO = T2.ONLN_MEMB_NO 
                       ) T2
                 WHERE  T1.PRMTNCD      = T2.PRMTNCD
                   AND  T1.STD_DT       = T2.STD_DT
                   AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
              ) ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_DELETE_01(온라인회원번호)  ************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FE_MK_PRMTN_ORD_SL T1
 WHERE EXISTS (
                SELECT 1
                  FROM ( SELECT DISTINCT STD_DT
                             , PRMTNCD
                             , ONLN_MEMB_NO
                          FROM FE_MK_PRMTN_ORD_TEMP  /* FE_MK_행사주문임시 */
                       ) T2
                 WHERE T1.PRMTNCD      = T2.PRMTNCD
                   AND T1.STD_DT       = T2.STD_DT
                   AND T1.ONLN_MEMB_NO = T2.ONLN_MEMB_NO
              ) ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

/* 구매행사 적재 --할인 */
INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FE_MK_PRMTN_ORD_SL T
SELECT /*+ USE_HASH(T1) PARALLEL(4) */
       T1.STD_DT
     , T1.PRMTNCD
     , T1.ONLN_ORD_NO
     , NVL(NVL(T1.INTG_MEMB_NO, T4.INTG_MEMB_NO),T1.ONLN_MEMB_NO)            AS INTG_MEMB_NO 
     , NVL(NVL(T1.INTG_MEMB_NO, T1.CUST_DISTING_NO),T1.ONLN_MEMB_NO)         AS INTG_CUST_DISTING_NO
     , T1.CUST_DISTING_NO
     , T1.LANG_CD
     , T1.DVIC_CD
     , T1.NATLT_CD
    /* , NVL(T99.RNKH_NATLT_CD,'z') AS RNKH_NATLT_CD  */ /*상위국적코드*/
     , CASE WHEN T3.ONLN_ORD_NO IS NOT NULL THEN 'Y' ELSE 'N' END            AS EXCH_PRMTN_APLY_YN
     , CASE WHEN T1.ONLN_ORD_DVS_CD IN ('06', '07') THEN 'Y' ELSE 'N' END    AS OMNI_ORD_YN
     , T1.ORD_MBSSYS_DVS_CD
     , T1.ONLN_MEMB_NO
     , NVL(T4.INTG_MEMB_GRD_CD,'z')                                          AS INTG_MEMB_GRD_CD
     , CASE WHEN T1.IMGN_DT IS NOT NULL THEN '0' ELSE 'z' END                AS IMGN_INFO_DVS_CD   /*출입국정보구분코드 0:출국*/
     , T1.IMGN_DT                    /*출입국일자 */
     , T5.LGPRCD
     , T5.MDPRCD
     , T5.PRMTN_LGCSF_CD
     , T5.PRMTN_MDCSF_CD
     , T1.DLR_NSALAMT     AS DLR_TOT_NSALAMT
     , T1.WON_NSALAMT     AS WON_TOT_NSALAMT
     , T1.DLR_DC_AMT      AS DLR_TOT_DC_AMT
     , T1.WON_DC_AMT      AS WON_TOT_DC_AMT
     , 0                  AS DLR_ACMLTMN_ARSE_NSALAMT  /* 적립금유발 */
     , 0                  AS WON_ACMLTMN_ARSE_NSALAMT
     , 0                  AS DLR_LDFP_ARSE_NSALAMT
     , 0                  AS WON_LDFP_ARSE_NSALAMT
     , 0                  AS DLR_CROS_NSALAMT
     , 0                  AS WON_CROS_NSALAMT
     , SYSDATE            AS LOAD_DTTM
  FROM (
        /* 회원행사매출 */
        SELECT /*+ PARALLEL(4) */
               T1.ONLN_ORD_NO
             , T1.ORD_DT        AS STD_DT
             , MAX(T1.INTG_MEMB_NO)  AS INTG_MEMB_NO
             , T2.PRMTNCD
             , MAX(T1.CUST_DISTING_NO   )                AS CUST_DISTING_NO
             , T1.ONLN_MEMB_NO                           AS ONLN_MEMB_NO
             , MAX(T1.LANG_CD           )                AS LANG_CD
             , MAX(T1.DVIC_CD           )                AS DVIC_CD
             , MAX(T1.ORD_MBSSYS_DVS_CD )                AS ORD_MBSSYS_DVS_CD
             , MAX(T1.NATLT_CD          )                AS NATLT_CD
             , MAX(T1.ONLN_ORD_DVS_CD   )                AS ONLN_ORD_DVS_CD
             , MAX(T1.DPTCTR_DT         )                AS IMGN_DT          /* 입출국일자 */
             , SUM(T1.GLBL_ORD_PYF_AMT*T1.SALES_SIGN)    AS DLR_NSALAMT
             , SUM(T1.LOCAL_ORD_PYF_AMT*T1.SALES_SIGN)   AS WON_NSALAMT
             , SUM(T1.GLBL_DC_AMT*T1.SALES_SIGN)         AS DLR_DC_AMT
             , SUM(T1.LOCAL_DC_AMT*T1.SALES_SIGN)        AS WON_DC_AMT
          FROM FE_SL_ORD_PROD T1 /* FE_SL_주문상품 */
         INNER JOIN (
              SELECT DISTINCT T1.STD_DT
                   , T1.ONLN_MEMB_NO
                   , T1.PRMTNCD
                FROM FE_MK_PRMTN_ORD_TEMP T1
             ) T2
            ON T1.ONLN_MEMB_NO = T2.ONLN_MEMB_NO
           AND T1.ORD_DT = T2.STD_DT
         INNER JOIN D_PRMTN T3
            ON T2.PRMTNCD = T3.PRMTNCD
           AND T1.STD_DT BETWEEN T3.PRMTN_STRT_DT AND T3.PRMTN_END_DT
         WHERE T3.PRMTN_LGCSF_CD IN ('001','014') /* 001:할인 014:딜 */
         GROUP BY T1.ONLN_ORD_NO
             , T1.ONLN_MEMB_NO
             , T1.ORD_DT
             , T2.PRMTNCD
     )  T1
  LEFT OUTER JOIN ( /* 주문행사매출 */
        SELECT /*+ PARALLEL(4) */
               T1.STD_DT
             , T1.PRMTNCD
             , T1.ONLN_ORD_NO
             --, T1.INTG_MEMB_NO
          FROM FE_MK_PRMTN_ORD_TEMP T1  /* FE_MK_행사주문임시 */
         GROUP BY T1.STD_DT
                , T1.PRMTNCD
                , T1.ONLN_ORD_NO
                --, T1.INTG_MEMB_NO
      /* 행사매출 교환권매출은 회원번호 기준으로 추출한뒤 통합회원번호로 적재 */
       ) T3
    ON T1.STD_DT       = T3.STD_DT
   AND T1.PRMTNCD      = T3.PRMTNCD
   AND T1.ONLN_ORD_NO  = T3.ONLN_ORD_NO
  -- AND T1.INTG_MEMB_NO = T3.INTG_MEMB_NO
 INNER JOIN D_INTG_MEMB T4  /* D_통합회원 */
    ON T1.ONLN_MEMB_NO = T4.ONLN_MEMB_NO
 INNER JOIN D_PRMTN  T5     /* D_행사 */
    ON T1.PRMTNCD      = T5.PRMTNCD
 WHERE T1.DLR_NSALAMT  >= 0
 ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_022  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

/* 구매행사 적재 --할인 이외 */
INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FE_MK_PRMTN_ORD_SL T
SELECT /*+ USE_HASH(T1) PARALLEL(4) */
       T1.STD_DT
     , T1.PRMTNCD
     , T1.ONLN_ORD_NO
     , NVL(NVL(T1.INTG_MEMB_NO, T4.INTG_MEMB_NO), T1.ONLN_MEMB_NO)           AS INTG_MEMB_NO 
     , NVL(NVL(T1.INTG_MEMB_NO, T1.CUST_DISTING_NO),T1.ONLN_MEMB_NO)         AS INTG_CUST_DISTING_NO
     , T1.CUST_DISTING_NO
     , T1.LANG_CD
     , T1.DVIC_CD
     , T1.NATLT_CD
    /* , NVL(T99.RNKH_NATLT_CD,'z') AS RNKH_NATLT_CD  */ /*상위국적코드*/
     , CASE WHEN T3.ONLN_ORD_NO IS NOT NULL THEN 'Y' ELSE 'N' END            AS EXCH_PRMTN_APLY_YN
     , CASE WHEN T1.ONLN_ORD_DVS_CD IN ('06', '07') THEN 'Y' ELSE 'N' END    AS OMNI_ORD_YN
     , T1.ORD_MBSSYS_DVS_CD
     , T1.ONLN_MEMB_NO
     , NVL(T4.INTG_MEMB_GRD_CD,'z')                                          AS INTG_MEMB_GRD_CD
     , CASE WHEN T1.IMGN_DT IS NOT NULL THEN '0' ELSE 'z' END                AS IMGN_INFO_DVS_CD   /*출입국정보구분코드 0:출국*/
     , T1.IMGN_DT                    /*출입국일자 */
     , T5.LGPRCD
     , T5.MDPRCD
     , T5.PRMTN_LGCSF_CD
     , T5.PRMTN_MDCSF_CD
     , CASE WHEN T1.STD_DT <= T5.PRMTN_END_DT THEN T1.DLR_NSALAMT ELSE 0 END AS DLR_NSALAMT
     , CASE WHEN T1.STD_DT <= T5.PRMTN_END_DT THEN T1.WON_NSALAMT ELSE 0 END AS WON_NSALAMT
     , CASE WHEN T1.STD_DT <= T5.PRMTN_END_DT THEN T1.DLR_DC_AMT  ELSE 0 END AS DLR_DC_AMT
     , CASE WHEN T1.STD_DT <= T5.PRMTN_END_DT THEN T1.WON_DC_AMT  ELSE 0 END AS WON_DC_AMT
     , CASE WHEN T1.DLR_ACMLTMN_PYF_AMT >0 AND T1.STD_DT >  T5.PRMTN_END_DT THEN T1.DLR_NSALAMT ELSE 0 END AS DLR_ACMLTMN_ARSE_NSALAMT  /* 적립금유발 */
     , CASE WHEN T1.DLR_ACMLTMN_PYF_AMT >0 AND T1.STD_DT >  T5.PRMTN_END_DT THEN T1.WON_NSALAMT ELSE 0 END AS WON_ACMLTMN_ARSE_NSALAMT
     , 0                                                                     AS DLR_LDFP_ARSE_NSALAMT
     , 0                                                                     AS WON_LDFP_ARSE_NSALAMT
     , 0                                                                     AS DLR_CROS_NSALAMT
     , 0                                                                     AS WON_CROS_NSALAMT
     , SYSDATE                                                               AS LOAD_DTTM
  FROM (
         SELECT /*+ PARALLEL(4) */
               T1.ONLN_ORD_NO
             , T1.ORD_DT        AS STD_DT
             , MAX(T1.INTG_MEMB_NO)  AS INTG_MEMB_NO
             , T2.PRMTNCD
             , MAX(T1.CUST_DISTING_NO   )                AS CUST_DISTING_NO
             , T1.ONLN_MEMB_NO                           AS ONLN_MEMB_NO
             , MAX(T1.LANG_CD           )                AS LANG_CD
             , MAX(T1.DVIC_CD           )                AS DVIC_CD
             , MAX(T1.ORD_MBSSYS_DVS_CD )                AS ORD_MBSSYS_DVS_CD
             , MAX(T1.NATLT_CD          )                AS NATLT_CD
             , MAX(T1.ONLN_ORD_DVS_CD   )                AS ONLN_ORD_DVS_CD
             , MAX(T1.DPTCTR_DT         )                AS IMGN_DT          /* 입출국일자 */
             , SUM(T1.GLBL_ORD_PYF_AMT*T1.SALES_SIGN)    AS DLR_NSALAMT
             , SUM(T1.LOCAL_ORD_PYF_AMT*T1.SALES_SIGN)   AS WON_NSALAMT
             , SUM(T1.GLBL_DC_AMT*T1.SALES_SIGN)         AS DLR_DC_AMT
             , SUM(T1.LOCAL_DC_AMT*T1.SALES_SIGN)        AS WON_DC_AMT
             , SUM(T1.DLR_ACMLTMN_PYF_AMT*T1.SALES_SIGN) AS DLR_ACMLTMN_PYF_AMT
          FROM FE_SL_ORD_PROD T1 /* FE_SL_주문상품 */
         INNER JOIN (
              SELECT DISTINCT STD_DT
                   , ONLN_MEMB_NO
                   , PRMTNCD
                FROM FE_MK_PRMTN_ORD_TEMP
             ) T2
            ON T1.ONLN_MEMB_NO = T2.ONLN_MEMB_NO
           AND T1.ORD_DT = T2.STD_DT
         INNER JOIN D_PRMTN T3
            ON T2.PRMTNCD = T3.PRMTNCD
	     AND T1.STD_DT BETWEEN T3.PRMTN_STRT_DT AND (CASE WHEN T3.PRMTN_END_DT < '99991201' THEN TO_CHAR(TO_DATE(T3.PRMTN_END_DT,'YYYYMMDD') + 30,'YYYYMMDD') ELSE T3.PRMTN_END_DT END)
         WHERE T3.PRMTN_LGCSF_CD NOT IN ('001','014') /* 001:할인 014:딜 */
         GROUP BY T1.ONLN_ORD_NO
             , T1.ONLN_MEMB_NO
             , T1.ORD_DT
             , T2.PRMTNCD
     )  T1
  LEFT OUTER JOIN ( /* 주문행사매출 */
       SELECT /*+ PARALLEL(4) */
              T1.STD_DT
            , T1.PRMTNCD
            , T1.ONLN_ORD_NO
            --, T1.INTG_MEMB_NO
         FROM FE_MK_PRMTN_ORD_TEMP T1  /* FE_MK_행사주문임시 */
        INNER JOIN D_PRMTN T2          /* D_행사 : 행사기간 이후의 유발매출은 교환권매출에서 제외하기 위함 */
           ON T1.PRMTNCD = T2.PRMTNCD
          AND T1.STD_DT  BETWEEN T2.PRMTN_STRT_DT AND T2.PRMTN_END_DT
        GROUP BY T1.STD_DT
               , T1.PRMTNCD
               , T1.ONLN_ORD_NO
               --, T1.INTG_MEMB_NO
       ) T3
    ON T1.STD_DT       = T3.STD_DT
   AND T1.PRMTNCD      = T3.PRMTNCD
   AND T1.ONLN_ORD_NO  = T3.ONLN_ORD_NO
   --AND T1.INTG_MEMB_NO = T3.INTG_MEMB_NO
  LEFT OUTER JOIN D_INTG_MEMB T4  /* D_통합회원 */
    ON T1.ONLN_MEMB_NO = T4.ONLN_MEMB_NO
 INNER JOIN D_PRMTN  T5     /* D_행사 */
    ON T1.PRMTNCD      = T5.PRMTNCD
 WHERE T1.DLR_NSALAMT  >= 0
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_03  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

/* LDFPAY 비구매적립 행사매출만 집계 */
INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FE_MK_PRMTN_ORD_SL T
SELECT /*+ PARALLEL(4) */
       T1.STD_DT
     , T1.PRMTNCD
     , '-999999999'                                                       AS ONLN_ORD_NO
     , T1.INTG_MEMB_NO
     , NVL(T1.INTG_MEMB_NO, T1.CUST_DISTING_NO)                           AS INTG_CUST_DISTING_NO
     , T1.CUST_DISTING_NO
     , T1.LANG_CD
     , T1.DVIC_CD
     , T1.NATLT_CD
    /* , NVL(T99.RNKH_NATLT_CD,'z') AS RNKH_NATLT_CD */ /*상위국적코드*/
     , 'N' AS EXCH_PRMTN_APLY_YN
     , CASE WHEN T1.ONLN_ORD_DVS_CD IN ('06', '07') THEN 'Y' ELSE 'N' END AS OMNI_ORD_YN
     , T1.ORD_MBSSYS_DVS_CD
     , T1.ONLN_MEMB_NO
     , NVL(T4.INTG_MEMB_GRD_CD,'z')                                       AS INTG_MEMB_GRD_CD
     , CASE WHEN T1.IMGN_DT IS NOT NULL THEN '0' ELSE 'z' END             AS IMGN_INFO_DVS_CD   /*출입국정보구분코드 0:출국*/
     , T1.IMGN_DT                       /*출입국일자 */
     , T5.LGPRCD
     , T5.MDPRCD
     , T5.PRMTN_LGCSF_CD
     , T5.PRMTN_MDCSF_CD
     , T1.DLR_NSALAMT
     , T1.WON_NSALAMT
     , T1.DLR_DC_AMT
     , T1.WON_DC_AMT
     , 0                                                                  AS DLR_ACMLTMN_ARSE_NSALAMT
     , 0                                                                  AS WON_ACMLTMN_ARSE_NSALAMT
     , 0                                                                  AS DLR_LDFP_ARSE_NSALAMT
     , 0                                                                  AS WON_LDFP_ARSE_NSALAMT
     , 0                                                                  AS DLR_CROS_NSALAMT
     , 0                                                                  AS WON_CROS_NSALAMT
     , SYSDATE                                                            AS LOAD_DTTM
  FROM (
        SELECT /*+ USE_HASH(T3) PARALLEL(4) */
               T2.STD_DT
             , T3.INTG_MEMB_NO
             , T3.PRMTNCD
             , MAX(T2.CUST_DISTING_NO   )              AS CUST_DISTING_NO
             , MAX(T2.ONLN_MEMB_NO      )              AS ONLN_MEMB_NO
             , MAX(T2.LANG_CD           )              AS LANG_CD
             , MAX(T2.DVIC_CD           )              AS DVIC_CD
             , MAX(T2.ORD_MBSSYS_DVS_CD )              AS ORD_MBSSYS_DVS_CD
             , MAX(T2.NATLT_CD          )              AS NATLT_CD
             , MAX(T2.ONLN_ORD_DVS_CD   )              AS ONLN_ORD_DVS_CD
             , MAX(T2.DPTCTR_DT         )              AS IMGN_DT          /* 입출국일자 */
             , SUM(T2.GLBL_ORD_PYF_AMT*T2.SALES_SIGN)  AS DLR_NSALAMT
             , SUM(T2.LOCAL_ORD_PYF_AMT*T2.SALES_SIGN) AS WON_NSALAMT
             , SUM(T2.GLBL_DC_AMT*T2.SALES_SIGN)       AS DLR_DC_AMT
             , SUM(T2.LOCAL_DC_AMT*T2.SALES_SIGN)      AS WON_DC_AMT
          FROM FE_SL_ORD_PROD T2 /* FE_SL_주문상품 */
         INNER JOIN
             (
               /*  LDFPAY 비구매적립  */
               SELECT /*+ USE_HASH(T6) PARALLEL(4) */
                      T0.LDFP_HIST_HAPN_DT            AS STD_DT
                    , CAST(T2.EVT_NO AS VARCHAR(10))  AS PRMTNCD
                    , T0.INTG_MEMB_NO
                    , SUM(T0.LDFP_ACMLT_AMT)          AS LDFP_ACMLT_AMT
                 FROM FL_MK_LDFP_ACMLT_USE_PTCLS  T0 /* FL_MK_LDFPAY적립사용내역 */
                INNER JOIN D_LDFP T1  /* D_LDFPAY */
                   ON T0.LDFP_NO = T1.LDFP_NO
                  AND T1.LDFP_PBLSH_DVS_CD  IN ('1','3')  /* LDF페이발행구분코드 1:온라인(정액) 3:온라인(정률) */
                INNER JOIN WE_MT_EVT_FVR T2  /* WE_행사혜택 */
                   ON T0.PRMTNCD = CAST(T2.LRWD_EVT_NO AS VARCHAR(10))  /* 온라인LDFP관련 행사코드는 LRWD_EVT_NO로 행사혜택의 EVT_NO를 찾아야 함.*/
                INNER JOIN WE_MT_OFR T4  /* WE_오퍼 */
                   ON T2.FVR_TGT_NO = T4.OFR_NO
                INNER JOIN WE_MT_EVT T5
                   ON T2.EVT_NO = T5.EVT_NO
                  AND T0.LDFP_HIST_HAPN_DT BETWEEN TO_CHAR(T5.EVT_STRT_DTIME,'YYYYMMDD') AND TO_CHAR(T5.EVT_END_DTIME,'YYYYMMDD')
                INNER JOIN (
                      SELECT /*+ PARALLEL(4) */
                             DISTINCT STD_DT
                           , PRMTNCD
                           , INTG_MEMB_NO
                        FROM FE_MK_PRMTN_ORD_TEMP
                       WHERE INTG_MEMB_NO IS NOT NULL 
                     ) T6
                   ON T6.STD_DT       = T0.LDFP_HIST_HAPN_DT
                  AND T6.PRMTNCD      = CAST(T2.EVT_NO AS VARCHAR(10))
                  AND T6.INTG_MEMB_NO = T0.INTG_MEMB_NO
                WHERE T0.LDFP_ACTI_YN     = 'Y'
                  AND T0.LDFP_NO_STAT_CD <> '00'
                  AND T0.LDFP_RCPDSBS_TYPE_CD IN ('11','24')    /* LDF페이수불유형코드 11:지급 24:취소(적립) 21:차감 26:취소(사용) */
                  AND T0.PRMTNCD          NOT IN ('11000002', '31000001') /* CS보상 ,정률기본적립 */
                GROUP BY T0.LDFP_HIST_HAPN_DT
                    , CAST(T2.EVT_NO AS VARCHAR(10))
                    , T0.INTG_MEMB_NO
             ) T3
            ON T2.STD_DT         = T3.STD_DT
           AND T2.INTG_MEMB_NO   = T3.INTG_MEMB_NO
         WHERE T3.LDFP_ACMLT_AMT <> 0
         GROUP BY T2.STD_DT
                , T3.INTG_MEMB_NO
                , T3.PRMTNCD
       ) T1
  INNER JOIN D_INTG_MEMB T4  /* D_통합회원 */
     ON T1.INTG_MEMB_NO = T4.INTG_MEMB_NO
  INNER JOIN D_PRMTN  T5     /* D_행사 */
     ON T1.PRMTNCD      = T5.PRMTNCD
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_MERGE_01  *****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

/* LDFPAY유발매출 */
MERGE /*+ APPEND PARALLEL(4) */
 INTO FE_MK_PRMTN_ORD_SL T1
USING (
       SELECT /*+ PARALLEL(4) */
              A1.STD_DT                                                        /* 기준일자               */
            , A1.PRMTNCD                                                       /* 행사코드               */
            , A1.ONLN_ORD_NO                                                   /* 온라인주문번호         */
            , A1.INTG_MEMB_NO                                                  /* 통합회원번호           */
            , NVL(A1.INTG_MEMB_NO,A1.CUST_DISTING_NO) AS INTG_CUST_DISTING_NO  /* 통합고객식별번호       */
            , A1.CUST_DISTING_NO                                               /* 고객식별번호           */
            , A1.LANG_CD                                                       /* 언어코드               */
            , A1.DVIC_CD                                                       /* 디바이스코드           */
            , A1.NATLT_CD                                                      /* 국적코드               */
          /*  , NVL(T99.RNKH_NATLT_CD,'z') AS RNKH_NATLT_CD  */ /*상위국적코드*/
            , 'N'   AS EXCH_PRMTN_APLY_YN                                      /* 교환권행사적용여부     */
            , CASE WHEN A1.ONLN_ORD_DVS_CD IN ('06', '07') THEN 'Y' ELSE 'N' END AS OMNI_ORD_YN  /* 옴니주문여부        */
            , A1.ORD_MBSSYS_DVS_CD                                             /* 주문회원제구분코드     */
            , A1.ONLN_MEMB_NO                                                  /* 온라인회원번호         */
            , A2.INTG_MEMB_GRD_CD                                              /* 통합회원등급코드       */
            , CASE WHEN A1.IMGN_DT IS NOT NULL THEN '0' ELSE 'z' END             AS IMGN_INFO_DVS_CD   /*출입국정보구분코드 0:출국*/
            , A1.IMGN_DT                                                       /* 출입국일자             */
            , A3.LGPRCD                                                        /* 대행사코드             */
            , A3.MDPRCD                                                        /* 중행사코드             */
            , A3.PRMTN_LGCSF_CD                                                /* 행사대분류코드         */
            , A3.PRMTN_MDCSF_CD                                                /* 행사중분류코드         */
            , 0      AS DLR_TOT_NSALAMT                                        /* 달러총순매출액         */
            , 0      AS WON_TOT_NSALAMT                                        /* 원화총순매출액         */
            , 0      AS DLR_TOT_DC_AMT                                         /* 달러총할인금액         */
            , 0      AS WON_TOT_DC_AMT                                         /* 원화총할인금액         */
            , 0      AS DLR_ACMLTMN_ARSE_NSALAMT                               /* 달러적립금유발순매출액 */
            , 0      AS WON_ACMLTMN_ARSE_NSALAMT                               /* 원화적립금유발순매출액 */
            , A1.DLR_LDFP_ARSE_NSALAMT                                         /* 달러LDFPAY유발순매출액 */
            , A1.WON_LDFP_ARSE_NSALAMT                                         /* 원화LDFPAY유발순매출액 */
            , A1.DLR_CROS_NSALAMT                                              /* 달러교차순매출액       */
            , A1.WON_CROS_NSALAMT                                              /* 원화교차순매출액       */
            , SYSDATE AS LOAD_DTTM
         FROM (
               SELECT /*+ USE_HASH(T1 T3) PARALLEL(4) */
                      T1.PRMTNCD
                    , T1.STD_DT
                    , T1.INTG_MEMB_NO
                    , T3.ONLN_ORD_NO
                    , MAX(T3.CUST_DISTING_NO   )              AS CUST_DISTING_NO
                    , MAX(T3.ONLN_MEMB_NO      )              AS ONLN_MEMB_NO
                    , MAX(T3.LANG_CD           )              AS LANG_CD
                    , MAX(T3.DVIC_CD           )              AS DVIC_CD
                    , MAX(T3.ORD_MBSSYS_DVS_CD )              AS ORD_MBSSYS_DVS_CD
                    , MAX(T3.NATLT_CD          )              AS NATLT_CD
                    , MAX(T3.ONLN_ORD_DVS_CD   )              AS ONLN_ORD_DVS_CD
                    , MAX(T3.DPTCTR_DT         )              AS IMGN_DT          /* 입출국일자 */
                    , SUM(T3.DLR_NSALAMT*T3.SALES_SIGN)       AS DLR_LDFP_ARSE_NSALAMT
                    , SUM(T3.WON_NSALAMT*T3.SALES_SIGN)       AS WON_LDFP_ARSE_NSALAMT
                    , SUM(CASE WHEN T1.SL_CHNL_CD NOT IN ('4','5') THEN T3.DLR_NSALAMT*T3.SALES_SIGN ELSE 0 END) AS DLR_CROS_NSALAMT
                    , SUM(CASE WHEN T1.SL_CHNL_CD NOT IN ('4','5') THEN T3.WON_NSALAMT*T3.SALES_SIGN ELSE 0 END) AS WON_CROS_NSALAMT
                 FROM (
                      SELECT  /*+ USE_HASH(T0 T3) PARALLEL(4) */
                             DISTINCT T0.LDFP_HIST_HAPN_DT   AS STD_DT
                           , CAST(T2.EVT_NO AS VARCHAR(10))  AS PRMTNCD
                           , T0.ONLN_ORD_NO                  AS ONLN_ORD_NO
                           , T0.INTG_MEMB_NO
                           , T0.SL_CHNL_CD
                           , SUM(T0.LDFP_USE_AMT) OVER(PARTITION BY T1.PRMTNCD, T0.ONLN_ORD_NO) AS LDFP_USE_AMT
                        FROM FL_MK_LDFP_ACMLT_USE_PTCLS  T0 /* FL_MK_LDFPAY적립사용내역 */
                        /*-- 변경적재 모수 ord_no조인------------*/
                       INNER JOIN (
                             SELECT DISTINCT ONLN_ORD_NO
                               FROM FE_MK_PRMTN_ORD_TEMP  ) T3
                          ON T0.ONLN_ORD_NO = T3.ONLN_ORD_NO
                        ---------------------------------------
                       INNER JOIN D_LDFP T1
                          ON T0.LDFP_NO = TRIM(T1.LDFP_NO)
                         AND T1.LDFP_PBLSH_DVS_CD  IN ('1','3')  /* LDF페이발행구분코드 1:온라인(정액) 3:온라인(정률) */
                       INNER JOIN WE_MT_EVT_FVR T2  /* WE_행사혜택 */
                          ON T0.PRMTNCD = CAST(T2.LRWD_EVT_NO AS VARCHAR(10))  /* 온라인LDFP관련 행사코드는 LRWD_EVT_NO로 행사혜택의 EVT_NO를 찾아야 함.*/
                       INNER JOIN WE_MT_OFR T4  /* WE_오퍼 */
                          ON T2.FVR_TGT_NO = T4.OFR_NO
                       INNER JOIN WE_MT_EVT T5
                       ON T2.EVT_NO = T5.EVT_NO
                         AND T0.LDFP_HIST_HAPN_DT BETWEEN TO_CHAR(T5.EVT_STRT_DTIME,'YYYYMMDD')
                                               AND (CASE WHEN TO_CHAR(T5.EVT_END_DTIME,'YYYYMMDD') < '99991201' THEN TO_CHAR( T5.EVT_END_DTIME + 30,'YYYYMMDD') ELSE TO_CHAR(T5.EVT_END_DTIME,'YYYYMMDD') END)
                       INNER JOIN D_INTG_MEMB T4
                          ON T0.INTG_MEMB_NO = TRIM(T4.INTG_MEMB_NO)
                       WHERE T0.LDFP_ACTI_YN = 'Y'
                         AND T0.LDFP_NO_STAT_CD <> '00'
                         AND T0.LDFP_RCPDSBS_TYPE_CD in('21','26')
                         AND T0.PRMTNCD NOT IN ('11000002', '31000001') /* CS보상 , 정률기본적립 */
                       ) T1
                 INNER JOIN FE_SL_ORD_PROD T3
                    ON T1.ONLN_ORD_NO = T3.ONLN_ORD_NO
                   AND T1.STD_DT = T3.STD_DT
                 WHERE T1.LDFP_USE_AMT > 0
                 GROUP BY T1.PRMTNCD
                     , T1.STD_DT
                     , T1.INTG_MEMB_NO
                     , T3.ONLN_ORD_NO
               ) A1
        INNER JOIN D_INTG_MEMB A2  /* D_통합회원 */
           ON A1.INTG_MEMB_NO = A2.INTG_MEMB_NO
        INNER JOIN D_PRMTN  A3     /* D_행사 */
           ON A1.PRMTNCD      = A3.PRMTNCD
     ) T2
ON (
        T1.STD_DT        = T2.STD_DT
    AND T1.PRMTNCD       = T2.PRMTNCD
    AND T1.ONLN_ORD_NO   = T2.ONLN_ORD_NO
    AND T1.INTG_MEMB_NO  = T2.INTG_MEMB_NO
   )
WHEN MATCHED THEN
 UPDATE SET T1.DLR_LDFP_ARSE_NSALAMT = T2.DLR_LDFP_ARSE_NSALAMT
          , T1.WON_LDFP_ARSE_NSALAMT = T2.WON_LDFP_ARSE_NSALAMT
          , T1.DLR_CROS_NSALAMT      = T2.DLR_CROS_NSALAMT
          , T1.WON_CROS_NSALAMT      = T2.WON_CROS_NSALAMT
          , T1.LOAD_DTTM             = SYSDATE
WHEN NOT MATCHED THEN
 INSERT (
          STD_DT                    /* 기준일자                  */
        , PRMTNCD                   /* 행사코드                  */
        , ONLN_ORD_NO               /* 온라인주문번호            */
        , INTG_MEMB_NO              /* 통합회원번호              */
        , INTG_CUST_DISTING_NO      /* 통합고객식별번호          */
        , CUST_DISTING_NO           /* 고객식별번호              */
        , LANG_CD                   /* 언어코드                  */
        , DVIC_CD                   /* 디바이스코드              */
        , NATLT_CD                  /* 국적코드                  */
        , EXCH_PRMTN_APLY_YN        /* 교환권행사적용여부        */
        , OMNI_ORD_YN               /* 옴니주문여부              */
        , ORD_MBSSYS_DVS_CD         /* 주문회원제구분코드        */
        , ONLN_MEMB_NO              /* 온라인회원번호            */
        , INTG_MEMB_GRD_CD          /* 통합회원등급코드          */
        , IMGN_INFO_DVS_CD          /* 출입국정보구분코드 1:출국 */
        , IMGN_DT                   /* 출입국일자                */
        , LGPRCD                    /* 대행사코드                */
        , MDPRCD                    /* 중행사코드                */
        , PRMTN_LGCSF_CD            /* 행사대분류코드            */
        , PRMTN_MDCSF_CD            /* 행사중분류코드            */
        , DLR_TOT_NSALAMT           /* 달러총순매출액            */
        , WON_TOT_NSALAMT           /* 원화총순매출액            */
        , DLR_TOT_DC_AMT            /* 달러총할인금액            */
        , WON_TOT_DC_AMT            /* 원화총할인금액            */
        , DLR_ACMLTMN_ARSE_NSALAMT  /* 달러적립금유발순매출액    */
        , WON_ACMLTMN_ARSE_NSALAMT  /* 원화적립금유발순매출액    */
        , DLR_LDFP_ARSE_NSALAMT     /* 달러LDFPAY유발순매출액    */
        , WON_LDFP_ARSE_NSALAMT     /* 원화LDFPAY유발순매출액    */
        , DLR_CROS_NSALAMT          /* 달러교차순매출액          */
        , WON_CROS_NSALAMT          /* 원화교차순매출액          */
        , LOAD_DTTM                 /* 적재일시                  */
        )
 VALUES (
          T2.STD_DT
        , T2.PRMTNCD
        , T2.ONLN_ORD_NO
        , T2.INTG_MEMB_NO
        , T2.INTG_CUST_DISTING_NO
        , T2.CUST_DISTING_NO
        , T2.LANG_CD
        , T2.DVIC_CD
        , T2.NATLT_CD
        , T2.EXCH_PRMTN_APLY_YN
        , T2.OMNI_ORD_YN
        , T2.ORD_MBSSYS_DVS_CD
        , T2.ONLN_MEMB_NO
        , T2.INTG_MEMB_GRD_CD
        , T2.IMGN_INFO_DVS_CD
        , T2.IMGN_DT
        , T2.LGPRCD
        , T2.MDPRCD
        , T2.PRMTN_LGCSF_CD
        , T2.PRMTN_MDCSF_CD
        , T2.DLR_TOT_NSALAMT
        , T2.WON_TOT_NSALAMT
        , T2.DLR_TOT_DC_AMT
        , T2.WON_TOT_DC_AMT
        , T2.DLR_ACMLTMN_ARSE_NSALAMT
        , T2.WON_ACMLTMN_ARSE_NSALAMT
        , T2.DLR_LDFP_ARSE_NSALAMT
        , T2.WON_LDFP_ARSE_NSALAMT
        , T2.DLR_CROS_NSALAMT
        , T2.WON_CROS_NSALAMT
        , SYSDATE
) ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;