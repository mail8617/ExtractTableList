/**********  UQ_CREATE_01  ****************************************************/


EXEC PR_CHK_DROP_TABLE('FL_MK_LGPROMO_CHNG_TEMP');

CREATE TABLE FL_MK_LGPROMO_CHNG_TEMP NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       DISTINCT T2.LGPRCD
     , T1.STR_CD
     , T1.INTG_MEMB_NO
     , SYSDATE      AS LOAD_DTTM
  FROM FL_MK_PRMTN_EXCH_CHNG_TEMP T1
 INNER JOIN D_PRMTN T2
    ON T1.PRMTNCD = T2.PRMTNCD
 WHERE T1.INTG_MEMB_NO IS NOT NULL
   AND T2.LGPRCD IS NOT NULL
;





/**********  UQ_CREATE_02  ****************************************************/


EXEC PR_CHK_DROP_TABLE('TEMP_LGPROMO_ACTRSLT_01');

/* 교환권관련 */
CREATE TABLE TEMP_LGPROMO_ACTRSLT_01 NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       T1.LGPRCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.INTG_CUST_DISTING_NO
     , T1.INTG_MEMB_NO
     , T1.CUST_DISTING_NO
     , T1.CUST_SALES_DVS_CD
     , T1.SL_CHNL_CD
     , T1.CUST_CLSF_CD
     , T1.GRP_CLSF_CD
     , T1.IMGN_INFO_DVS_CD       /* 출입국정보구분코드*/
     , T1.IMGN_DT                /* 출입국일자 */
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.DLR_PDCST*T1.SALES_SIGN ELSE 0 END)                AS DLR_PDCST_SUM_AMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.WON_PDCST*T1.SALES_SIGN ELSE 0 END)                AS WON_PDCST_SUM_AMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS DLR_EXCH_NSALAMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS WON_EXCH_NSALAMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.SALES_SIGN ELSE 0 END)                             AS EXCH_CNT
     , SUM(ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0))                                                       AS DLR_PRMTN_NSALAMT
     , SUM(ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0))                                                       AS WON_PRMTN_NSALAMT
     , SUM(T1.SALES_SIGN)                                                                                   AS PRMTN_EXCH_CNT
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END)     AS OMNI_DLR_NSALAMT        /* 옴니달러순매출액     */
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END)     AS OMNI_WON_NSALAMT        /* 옴니원화순매출액     */
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' AND EXCH_PRMTN_APLY_YN ='Y' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_DLR_EXCH_NSALAMT /* 옴니달러교환권순매출액 */
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' AND EXCH_PRMTN_APLY_YN ='Y' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_WON_EXCH_NSALAMT /* 옴니원화교환권순매출액 */
     -------
     , SUM(T3.DLR_ARSE_NSALAMT*T1.SALES_SIGN)                                                               AS DLR_ARSE_NSALAMT        /* 달러유발순매출액     */
     , SUM(T3.WON_ARSE_NSALAMT*T1.SALES_SIGN)                                                               AS WON_ARSE_NSALAMT        /* 원화유발순매출액     */
     , SUM(CASE WHEN T1.SL_CHNL_CD IN ('4','5') THEN T3.DLR_ARSE_NSALAMT*T1.SALES_SIGN ELSE 0 END)          AS DLR_CROS_ARSE_NSALAMT   /* 달러교차유발순매출액 */
     , SUM(CASE WHEN T1.SL_CHNL_CD IN ('4','5') THEN T3.WON_ARSE_NSALAMT*T1.SALES_SIGN ELSE 0 END)          AS WON_CROS_ARSE_NSALAMT   /* 원화교차유발순매출액 */
  FROM
     (
       SELECT /*+ USE_HASH(T2) PARALLEL(4) */
              T1.STR_CD
            , T1.STD_DT
            , T1.EXCH_NO
            , T1.INTG_CUST_DISTING_NO
            , T1.INTG_MEMB_NO
            , T1.CUST_DISTING_NO
            , T1.CUST_SALES_DVS_CD
            , T1.SL_CHNL_CD
            , T1.CUST_CLSF_CD
            , T1.GRP_CLSF_CD
            , T1.LGPRCD
            , T1.EXCH_PRMTN_APLY_YN
            , NVL(T3.DLR_PDCST,0) AS DLR_PDCST
            , NVL(T3.WON_PDCST,0) AS WON_PDCST
            , T1.DLR_TOT_NSALAMT
            , T1.WON_TOT_NSALAMT
            , T1.SALES_SIGN
            , T1.NATLT_CD
            , T1.ONLN_SL_DVS_CD
            , T1.IMGN_INFO_DVS_CD       /* 출입국정보구분코드*/
            , T1.IMGN_DT                /* 출입국일자 */
            , T1.DLR_ARSE_NSALAMT
            , T1.WON_ARSE_NSALAMT
            , ROW_NUMBER() OVER (PARTITION BY T1.LGPRCD, T1.STR_CD, T1.STD_DT, T1.EXCH_NO ORDER BY T1.EXCH_PRMTN_APLY_YN DESC, T1.PRMTNCD) AS RN
         FROM FL_MK_PRMTN_EXCH_SL T1           /* FL_MK_행사교환권판매 */
        INNER JOIN FL_MK_LGPROMO_CHNG_TEMP T2  /* 변경적재 대상 */
           ON T1.LGPRCD       = T2.LGPRCD
          AND T1.STR_CD       = T2.STR_CD
          AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
         LEFT OUTER JOIN (  /* 원가금액 가져오기 */
              SELECT T1.STR_CD
                   , T1.SL_DT
                   , T1.EXCH_NO
                   , SUM(T1.DLR_PDCST*T1.SL_QTY)  AS DLR_PDCST
                   , SUM(T1.WON_PDCST*T1.SL_QTY)  AS WON_PDCST
                FROM FL_SL_PROD_SL T1  /* FL_SL_상품판매 */
               INNER JOIN ( /* 중행사 관련 적재 대상 고객의 전체 교환권 */
                     SELECT /*+ FULL(T1) PARALLEL(4) */
                            DISTINCT T1.STD_DT
                          , T1.STR_CD
                          , T1.EXCH_NO
                       FROM FL_MK_PRMTN_EXCH_SL T1 /* FL_MK_행사교환권판매 */
                      INNER JOIN (
                            SELECT DISTINCT  T2.LGPRCD
                                 , T1.STR_CD
                                 , T1.INTG_MEMB_NO
                              FROM FL_MK_PRMTN_EXCH_CHNG_TEMP T1
                             INNER JOIN D_PRMTN T2
                                ON T1.PRMTNCD = T2.PRMTNCD
                           ) T2
                         ON T1.LGPRCD       = T2.LGPRCD
                        AND T1.STR_CD       = T2.STR_CD
                        AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
                    )   T2  /* 매출변경건 */
                  ON T1.SL_DT   = T2.STD_DT
                 AND T1.STR_CD  = T2.STR_CD
                 AND T1.EXCH_NO = T2.EXCH_NO
               GROUP BY T1.STR_CD
                      , T1.SL_DT
                      , T1.EXCH_NO
               )  T3
           ON T1.STR_CD  = T3.STR_CD
          AND T1.STD_DT  = T3.SL_DT
          AND T1.EXCH_NO = T3.EXCH_NO
    ) T1
  LEFT OUTER JOIN (
       SELECT /*+ USE_HASH(T2) PARALLEL(4) */
              T1.STR_CD
            , T1.STD_DT
            , T1.EXCH_NO
            , T1.INTG_MEMB_NO
            , T1.LGPRCD
            , T1.DLR_ARSE_NSALAMT
            , T1.WON_ARSE_NSALAMT
            , ROW_NUMBER() OVER (PARTITION BY T1.LGPRCD, T1.STR_CD, T1.STD_DT, T1.EXCH_NO ORDER BY T1.PRMTNCD) AS RNUM
         FROM FL_MK_PRMTN_EXCH_SL T1            /* FL_MK_행사교환권판매 */
        INNER JOIN FL_MK_LGPROMO_CHNG_TEMP T2  /* 변경적재 대상 */
           ON T1.LGPRCD       = T2.LGPRCD
          AND T1.STR_CD       = T2.STR_CD
          AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
        WHERE T1.DLR_ARSE_NSALAMT <> 0
      )T3
    ON T1.STR_CD  = T3.STR_CD
   AND T1.STD_DT  = T3.STD_DT
   AND T1.EXCH_NO = T3.EXCH_NO
   AND T1.LGPRCD  = T3.LGPRCD
   AND T3.RNUM = 1
 WHERE RN = 1
 GROUP BY T1.LGPRCD
        , T1.STR_CD
        , T1.STD_DT
        , T1.INTG_CUST_DISTING_NO
        , T1.INTG_MEMB_NO
        , T1.CUST_DISTING_NO
        , T1.CUST_SALES_DVS_CD
        , T1.SL_CHNL_CD
        , T1.CUST_CLSF_CD
        , T1.GRP_CLSF_CD
        , T1.IMGN_INFO_DVS_CD
        , T1.IMGN_DT
;





/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FL_MK_LGPROMO_ACTRSLT T1
 WHERE EXISTS (
               SELECT 1
                 FROM FL_MK_LGPROMO_CHNG_TEMP T0
                WHERE T1.STR_CD        = T0.STR_CD
                  AND T1.LGPRCD        = T0.LGPRCD
                  AND T1.INTG_MEMB_NO  = T0.INTG_MEMB_NO
              );

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FL_MK_LGPROMO_ACTRSLT T
SELECT /*+ PARALLEL(4) */
       T1.LGPRCD                  /* 대행사코드             */
     , T1.STR_CD                  /* 점코드               */
     , T1.STD_DT                  /* 기준일자              */
     , T1.INTG_MEMB_NO            /* 통합회원번호           */
     , T1.INTG_CUST_DISTING_NO
     , CASE WHEN T1.INTG_MEMB_NO IS NOT NULL THEN 'Y' ELSE 'N' END AS INTG_MEMB_YN
     , T1.CUST_DISTING_NO         /* 고객식별번호           */
    -- , CASE WHEN T1.CUST_SALES_DVS_CD = '1' THEN T2.PSPT_RCGNT_NO
    --        WHEN T1.CUST_SALES_DVS_CD = '2' THEN TO_NUMBER(TRIM(SUBSTR(T1.CUST_DISTING_NO,4,9) ))
    --        ELSE  0   END         AS PSPT_RCGNT_NO  /* 여권인식번호           */
     , NVL(T6.PSPT_RCGNT_NO, T2.PSPT_RCGNT_NO)  AS PSPT_RCGNT_NO             /* 여권인식번호           */
      /************
     , CASE WHEN TRANSLATE(SUBSTR(T4.PSPT_SHTN_NO,1,4),'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ','999999999999999999999999999999999999') =  '9999'
            THEN 'Y' ELSE 'N' END PSPTNO_NML_YN
      ********/
     , CASE WHEN REGEXP_COUNT(SUBSTR(T4.PSPT_SHTN_NO,1,4),'[0-9|A-Z]') = 4 THEN 'Y'
            ELSE 'N'
       END                                AS PSPTNO_NML_YN           /* 여권번호정상여부         */
     , NVL(T1.CUST_SALES_DVS_CD     ,'z') AS CUST_SALES_DVS_CD       /* 고객매출구분코드         */
     , NVL(T1.SL_CHNL_CD,'z')             AS SL_CHNL_CD              /* 판매채널코드             */
     , NVL(T2.MEMB_DVS_CD           ,'z') AS MEMB_DVS_CD             /* 회원구분코드             */
     , 'z'                                AS VIP_CARD_DVS_CD         /* VIP카드구분코드          */
    -- , CASE WHEN T2.VIP_NO IS NOT NULL THEN DECODE(T2.NATLT_CD,'z','KOR',T2.NATLT_CD)
    --            WHEN T1.CUST_SALES_DVS_CD = '2' THEN SUBSTR(T1.CUST_DISTING_NO,1,3)
    --            ELSE 'KOR'  END           AS NATLT_CD                /* 국적코드                 */
     , NVL(T6.NATLT_CD,'z')               AS NATLT_CD               /* 국적코드            */
     , NVL(T2.NTV_FORN_DVS_CD       ,'z') AS NTV_FORN_DVS_CD         /* 내국인외국인구분코드     */
     , NVL(T2.RESID_STD_NATLT_DVS_CD,'z') AS RESID_STD_NATLT_DVS_CD  /* 거주기준국적구분코드     */
     , NVL(T6.SEX_CD                ,'z') AS SEX_CD                  /* 성별코드                 */
     , NVL(T6.AGE_CD                ,999) AS AGE_CD                  /* 연령코드                 */
     ,  'z'                               AS FRNCTR_RGN_DVS_CD       /* 외국지역구분코드         */
     , NVL(T6.INTG_MEMB_GRD_CD      ,'z') AS INTG_MEMB_GRD_CD        /* 통합회원등급코드         */
     , NVL(T2.WDRMBSHIP_YN          ,'z') AS WDRMBSHIP_YN            /* 탈회여부                 */
     , NVL(T1.CUST_CLSF_CD          ,'z') AS CUST_CLSF_CD
     , NVL(T1.GRP_CLSF_CD           ,'z') AS GRP_CLSF_CD
     , NVL(T1.IMGN_INFO_DVS_CD     ,'z') AS IMGN_INFO_DVS_CD         /* 출입국정보구분코드*/
     , T1.IMGN_DT                                                    /* 출입국일자 */
     , NVL(T1.DLR_PDCST_SUM_AMT      , 0) AS DLR_PDCST_SUM_AMT       /* 달러원가합계금액         */
     , NVL(T1.WON_PDCST_SUM_AMT      , 0) AS WON_PDCST_SUM_AMT       /* 원화원가합계금액         */
     , NVL(T1.DLR_EXCH_NSALAMT       , 0) AS DLR_EXCH_NSALAMT        /* 달러교환권순매출액       */
     , NVL(T1.WON_EXCH_NSALAMT       , 0) AS WON_EXCH_NSALAMT        /* 원화교환권순매출액       */
     , NVL(T1.EXCH_CNT               , 0) AS EXCH_CNT                /* 교환권건수               */
     , NVL(T1.DLR_PRMTN_NSALAMT      , 0) AS DLR_PRMTN_NSALAMT       /* 달러행사순매출액         */
     , NVL(T1.WON_PRMTN_NSALAMT      , 0) AS WON_PRMTN_NSALAMT       /* 원화행사순매출액         */
     , NVL(T1.PRMTN_EXCH_CNT         , 0) AS PRMTN_EXCH_CNT          /* 행사교환권건수           */
     , NVL(T1.DLR_ARSE_NSALAMT       , 0) AS DLR_ARSE_NSALAMT        /* 달러유발순매출액         */
     , NVL(T1.WON_ARSE_NSALAMT       , 0) AS WON_ARSE_NSALAMT        /* 원화유발순매출액         */
     , NVL(T1.DLR_PRMTN_DC_AMT       , 0) AS DLR_PRMTN_DC_AMT        /* 달러행사할인금액         */
     , NVL(T1.WON_PRMTN_DC_AMT       , 0) AS WON_PRMTN_DC_AMT        /* 원화행사할인금액         */
     , NVL(T1.DC_OURCOM_BDN_AMT      , 0) AS DC_OURCOM_BDN_AMT       /* 할인당사부담금액         */
     , NVL(T1.DC_ALYCO_BDN_AMT       , 0) AS DC_ALYCO_BDN_AMT        /* 할인제휴사부담금액       */
     , NVL(T1.DC_CNT                 , 0) AS DC_CNT                  /* 할인건수                 */
     , NVL(T1.WON_TOT_DC_AMT         , 0) AS WON_TOT_DC_AMT          /* 원화총할인금액           */
     , NVL(T1.FGF_PRESTAT_AMT        , 0) AS FGF_PRESTAT_AMT         /* 사은품증정금액           */
     , NVL(T1.FGF_PRESTAT_CNT        , 0) AS FGF_PRESTAT_CNT         /* 사은품증정건수           */
     , NVL(T1.PRESTAT_OURCOM_BDN_AMT , 0) AS PRESTAT_OURCOM_BDN_AMT  /* 증정당사부담금액         */
     , NVL(T1.PRESTAT_ALYCO_BDN_AMT  , 0) AS PRESTAT_ALYCO_BDN_AMT   /* 증정제휴사부담금액       */
     , NVL(T1.TOT_PRESTAT_AMT        , 0) AS TOT_PRESTAT_AMT         /* 총증정금액               */
     , NVL(T1.LDFP_ACMLT_AMT         , 0) AS LDFP_ACMLT_AMT          /* LDFPAY적립금액           */
     , NVL(T1.LDFP_ACMLT_CNT         , 0) AS LDFP_ACMLT_CNT          /* LDFPAY적립건수           */
     , NVL(T1.LDFP_OURCOM_BDN_AMT    , 0) AS LDFP_OURCOM_BDN_AMT     /* LDFPAY당사부담금액       */
     , NVL(T1.LDFP_ALYCO_BDN_AMT     , 0) AS LDFP_ALYCO_BDN_AMT      /* LDFPAY제휴사부담금액     */
     , NVL(T1.FREELDFP_USE_AMT       , 0) AS FREELDFP_USE_AMT        /* 프리LDFPAY사용금액       */
     , NVL(T1.FREELDFP_USE_CNT       , 0) AS FREELDFP_USE_CNT        /* 프리LDFPAY사용건수       */
     , NVL(T1.FREELDFP_OURCOM_BDN_AMT, 0) AS FREELDFP_OURCOM_BDN_AMT /* 프리LDFPAY당사부담금액   */
     , NVL(T1.FREELDFP_ALYCO_BDN_AMT , 0) AS FREELDFP_ALYCO_BDN_AMT  /* 프리LDFPAY제휴사부담금액 */
     , NVL(T1.DLR_CROS_ARSE_NSALAMT  , 0) AS DLR_CROS_ARSE_NSALAMT   /* 달러교차유발순매출액     */
     , NVL(T1.WON_CROS_ARSE_NSALAMT  , 0) AS WON_CROS_ARSE_NSALAMT   /* 원화교차유발순매출액     */
     , NVL(T1.OMNI_DLR_NSALAMT       , 0) AS OMNI_DLR_NSALAMT        /* 옴니달러순매출액         */
     , NVL(T1.OMNI_WON_NSALAMT       , 0) AS OMNI_WON_NSALAMT        /* 옴니원화순매출액         */
     , NVL(T1.OMNI_DLR_EXCH_NSALAMT  , 0) AS OMNI_DLR_EXCH_NSALAMT   /* 옴니달러교환권순매출액   */
     , NVL(T1.OMNI_WON_EXCH_NSALAMT  , 0) AS OMNI_WON_EXCH_NSALAMT   /* 옴니원화교환권순매출액   */
     , NVL(T1.FREELDFP_ACMLT_AMT      ,0) AS FREELDFP_ACMLT_AMT      /* 프리LDFPAY적립금액 */
     , NVL(T1.FREELDFP_ACMLT_CNT      ,0) AS FREELDFP_ACMLT_CNT      /* 프리LDFPAY적립건수 */
     , SYSDATE                            AS LOAD_DTTM               /* 적재일시                 */
     , 0                                 AS DLR_CROS_EXCH_NSALAMT   /*달러교차교환권순매출액   */
     , 0                                 AS WON_CROS_EXCH_NSALAMT   /*원화교차교환권순매출액   */
  FROM (
        SELECT /*+ PARALLEL(4) */
               LGPRCD                           AS LGPRCD
             , STR_CD                           AS STR_CD
             , STD_DT                           AS STD_DT
             , MAX(INTG_CUST_DISTING_NO)        AS INTG_CUST_DISTING_NO
             , INTG_MEMB_NO                     AS INTG_MEMB_NO
             , MAX(CUST_DISTING_NO       )      AS CUST_DISTING_NO
             , MAX(CUST_SALES_DVS_CD     )      AS CUST_SALES_DVS_CD
             , MAX(SL_CHNL_CD            )      AS SL_CHNL_CD
             , MAX(CUST_CLSF_CD          )      AS CUST_CLSF_CD
             , MAX(GRP_CLSF_CD           )      AS GRP_CLSF_CD
             , MAX(T1.IMGN_INFO_DVS_CD   )      AS IMGN_INFO_DVS_CD
             , MAX(T1.IMGN_DT            )      AS IMGN_DT
             , SUM(DLR_PDCST_SUM_AMT     )      AS DLR_PDCST_SUM_AMT
             , SUM(WON_PDCST_SUM_AMT     )      AS WON_PDCST_SUM_AMT
             , SUM(DLR_EXCH_NSALAMT      )      AS DLR_EXCH_NSALAMT
             , SUM(WON_EXCH_NSALAMT      )      AS WON_EXCH_NSALAMT
             , SUM(EXCH_CNT              )      AS EXCH_CNT
             , SUM(DLR_PRMTN_NSALAMT     )      AS DLR_PRMTN_NSALAMT
             , SUM(WON_PRMTN_NSALAMT     )      AS WON_PRMTN_NSALAMT
             , SUM(PRMTN_EXCH_CNT        )      AS PRMTN_EXCH_CNT
             , SUM(DLR_ARSE_NSALAMT      )      AS DLR_ARSE_NSALAMT
             , SUM(WON_ARSE_NSALAMT      )      AS WON_ARSE_NSALAMT
             , SUM(DLR_PRMTN_DC_AMT      )      AS DLR_PRMTN_DC_AMT
             , SUM(WON_PRMTN_DC_AMT      )      AS WON_PRMTN_DC_AMT
             , SUM(DC_OURCOM_BDN_AMT     )      AS DC_OURCOM_BDN_AMT
             , SUM(DC_ALYCO_BDN_AMT      )      AS DC_ALYCO_BDN_AMT
             , SUM(DC_CNT                )      AS DC_CNT
             , SUM(WON_TOT_DC_AMT        )      AS WON_TOT_DC_AMT
             , SUM(FGF_PRESTAT_AMT       )      AS FGF_PRESTAT_AMT
             , SUM(FGF_PRESTAT_CNT       )      AS FGF_PRESTAT_CNT
             , SUM(PRESTAT_OURCOM_BDN_AMT)      AS PRESTAT_OURCOM_BDN_AMT
             , SUM(PRESTAT_ALYCO_BDN_AMT )      AS PRESTAT_ALYCO_BDN_AMT
             , SUM(TOT_PRESTAT_AMT       )      AS TOT_PRESTAT_AMT
             , SUM(LDFP_ACMLT_AMT        )      AS LDFP_ACMLT_AMT
             , SUM(LDFP_ACMLT_CNT        )      AS LDFP_ACMLT_CNT
             , SUM(LDFP_OURCOM_BDN_AMT   )      AS LDFP_OURCOM_BDN_AMT
             , SUM(LDFP_ALYCO_BDN_AMT    )      AS LDFP_ALYCO_BDN_AMT
             , SUM(FREELDFP_USE_AMT       )     AS FREELDFP_USE_AMT
             , SUM(FREELDFP_USE_CNT       )     AS FREELDFP_USE_CNT
             , SUM(FREELDFP_OURCOM_BDN_AMT)     AS FREELDFP_OURCOM_BDN_AMT
             , SUM(FREELDFP_ALYCO_BDN_AMT )     AS FREELDFP_ALYCO_BDN_AMT
             , SUM(DLR_CROS_ARSE_NSALAMT  )     AS DLR_CROS_ARSE_NSALAMT
             , SUM(WON_CROS_ARSE_NSALAMT  )     AS WON_CROS_ARSE_NSALAMT
             /* 20220803 옴니채널 추가 */
             , SUM(OMNI_DLR_NSALAMT      )      AS OMNI_DLR_NSALAMT
             , SUM(OMNI_WON_NSALAMT      )      AS OMNI_WON_NSALAMT
             , SUM(OMNI_DLR_EXCH_NSALAMT )      AS OMNI_DLR_EXCH_NSALAMT
             , SUM(OMNI_WON_EXCH_NSALAMT )      AS OMNI_WON_EXCH_NSALAMT
         , SUM(FREELDFP_ACMLT_AMT     )      AS FREELDFP_ACMLT_AMT
         , SUM(FREELDFP_ACMLT_CNT     )      AS FREELDFP_ACMLT_CNT
          FROM ( /* 교환권 관련 매출 */
              SELECT /*+ PARALLEL(4) */
                     T1.LGPRCD
                   , T1.STR_CD
                   , T1.STD_DT
                   , T1.INTG_CUST_DISTING_NO
                   , T1.INTG_MEMB_NO
                   , T1.CUST_DISTING_NO
                   , T1.CUST_SALES_DVS_CD
                   , T1.SL_CHNL_CD
                   , T1.CUST_CLSF_CD
                   , T1.GRP_CLSF_CD
                   , T1.IMGN_INFO_DVS_CD
                   , T1.IMGN_DT
                   , T1.DLR_PDCST_SUM_AMT          /* 달러원가합계금액          */
                   , T1.WON_PDCST_SUM_AMT          /* 원화원가합계금액          */
                   , T1.DLR_EXCH_NSALAMT           /* 달러교환권순매출액        */
                   , T1.WON_EXCH_NSALAMT           /* 원화교환권순매출액        */
                   , T1.EXCH_CNT                   /* 교환권건수                */
                   , T1.DLR_PRMTN_NSALAMT          /* 달러행사순매출액          */
                   , T1.WON_PRMTN_NSALAMT          /* 원화행사순매출액          */
                   , T1.PRMTN_EXCH_CNT             /* 행사교환권건수            */
                   , T1.DLR_ARSE_NSALAMT           /* 달러유발순매출액          */
                   , T1.WON_ARSE_NSALAMT           /* 원화유발순매출액          */
                   , 0  AS DLR_PRMTN_DC_AMT        /* 달러행사할인금액          */
                   , 0  AS WON_PRMTN_DC_AMT        /* 원화행사할인금액          */
                   , 0  AS DC_OURCOM_BDN_AMT       /* 할인당사부담금액          */
                   , 0  AS DC_ALYCO_BDN_AMT        /* 할인제휴사부담금액        */
                   , 0  AS DC_CNT                  /* 할인건수                  */
                   , 0  AS WON_TOT_DC_AMT          /* 원화총할인금액            */
                   , 0  AS FGF_PRESTAT_AMT         /* 사은품증정금액            */
                   , 0  AS FGF_PRESTAT_CNT         /* 사은품증정건수            */
                   , 0  AS PRESTAT_OURCOM_BDN_AMT  /* 증정당사부담금액          */
                   , 0  AS PRESTAT_ALYCO_BDN_AMT   /* 증정제휴사부담금액        */
                   , 0  AS TOT_PRESTAT_AMT         /* 총증정금액                */
                   , 0  AS LDFP_ACMLT_AMT          /* LDFPAY적립금액            */
                   , 0  AS LDFP_ACMLT_CNT          /* LDFPAY적립건수            */
                   , 0  AS LDFP_OURCOM_BDN_AMT     /* LDFPAY당사부담금액        */
                   , 0  AS LDFP_ALYCO_BDN_AMT      /* LDFPAY제휴사부담금액      */
                   , 0  AS FREELDFP_USE_AMT        /* 프리LDFPAY사용금액        */
                   , 0  AS FREELDFP_USE_CNT        /* 프리LDFPAY사용건수        */
                   , 0  AS FREELDFP_OURCOM_BDN_AMT /* 프리LDFPAY당사부담금액    */
                   , 0  AS FREELDFP_ALYCO_BDN_AMT  /* 프리LDFPAY제휴사부담금액  */
                   , T1.DLR_CROS_ARSE_NSALAMT      /* 달러교차유발순매출액      */
                   , T1.WON_CROS_ARSE_NSALAMT      /* 원화교차유발순매출액      */
                   , T1.OMNI_DLR_NSALAMT           /* 옴니달러순매출액          */
                   , T1.OMNI_WON_NSALAMT           /* 옴니원화순매출액          */
                   , T1.OMNI_DLR_EXCH_NSALAMT      /* 옴니달러교환권순매출액    */
                   , T1.OMNI_WON_EXCH_NSALAMT      /* 옴니원화교환권순매출액    */
             , 0  AS FREELDFP_ACMLT_AMT      /* 프리LDFPAY적립금액 */
               , 0  AS FREELDFP_ACMLT_CNT      /* 프리LDFPAY적립건수 */
                FROM TEMP_LGPROMO_ACTRSLT_01 T1
               UNION ALL
              /* 할인금액 */
              SELECT /*+ PARALLEL(4) */
                     T2.LGPRCD
                   , T2.STR_CD
                   , T2.STD_DT
                   , T2.INTG_CUST_DISTING_NO
                   , T2.INTG_MEMB_NO
                   , T2.CUST_DISTING_NO
                   , T2.CUST_SALES_DVS_CD
                   , T2.SL_CHNL_CD
                   , T2.CUST_CLSF_CD
                   , T2.GRP_CLSF_CD
                   , T2.IMGN_INFO_DVS_CD
                   , T2.IMGN_DT
                   , 0                               AS DLR_PDCST_SUM_AMT       /* 달러원가합계금액        */
                   , 0                               AS WON_PDCST_SUM_AMT       /* 원화원가합계금액        */
                   , 0                               AS DLR_EXCH_NSALAMT        /* 달러교환권순매출액      */
                   , 0                               AS WON_EXCH_NSALAMT        /* 원화교환권순매출액      */
                   , 0                               AS EXCH_CNT                /* 교환권건수              */
                   , 0                               AS DLR_PRMTN_NSALAMT       /* 달러행사순매출액        */
                   , 0                               AS WON_PRMTN_NSALAMT       /* 원화행사순매출액        */
                   , 0                               AS PRMTN_EXCH_CNT          /* 행사교환권건수          */
                   , 0                               AS DLR_ARSE_NSALAMT        /* 달러유발순매출액        */
                   , 0                               AS WON_ARSE_NSALAMT        /* 원화유발순매출액        */
                   , SUM(T2.DLR_PRMTN_DC_AMT       ) AS DLR_PRMTN_DC_AMT        /* 달러행사할인금액         */
                   , SUM(T2.WON_PRMTN_DC_AMT       ) AS WON_PRMTN_DC_AMT        /* 원화행사할인금액         */
                   , SUM(T2.DC_OURCOM_BDN_AMT      ) AS DC_OURCOM_BDN_AMT       /* 할인당사부담금액         */
                   , SUM(T2.DC_ALYCO_BDN_AMT       ) AS DC_ALYCO_BDN_AMT        /* 할인제휴사부담금액       */
                   , SUM(T2.DC_CNT                 ) AS DC_CNT                  /* 할인건수                 */
                   , SUM(T2.WON_TOT_DC_AMT         ) AS WON_TOT_DC_AMT          /* 원화총할인금액           */
                   , SUM(T2.FGF_PRESTAT_AMT        ) AS FGF_PRESTAT_AMT         /* 사은품증정금액           */
                   , SUM(T2.FGF_PRESTAT_CNT        ) AS FGF_PRESTAT_CNT         /* 사은품증정건수           */
                   , SUM(T2.PRESTAT_OURCOM_BDN_AMT ) AS PRESTAT_OURCOM_BDN_AMT  /* 증정당사부담금액         */
                   , SUM(T2.PRESTAT_ALYCO_BDN_AMT  ) AS PRESTAT_ALYCO_BDN_AMT   /* 증정제휴사부담금액       */
                   , SUM(T2.TOT_PRESTAT_AMT        ) AS TOT_PRESTAT_AMT         /* 총증정금액               */
                   , SUM(T2.LDFP_ACMLT_AMT         ) AS LDFP_ACMLT_AMT          /* LDFPAY적립금액           */
                   , SUM(T2.LDFP_ACMLT_CNT         ) AS LDFP_ACMLT_CNT          /* LDFPAY적립건수           */
                   , SUM(T2.LDFP_OURCOM_BDN_AMT    ) AS LDFP_OURCOM_BDN_AMT     /* LDFPAY당사부담금액       */
                   , SUM(T2.LDFP_ALYCO_BDN_AMT     ) AS LDFP_ALYCO_BDN_AMT      /* LDFPAY제휴사부담금액     */
                   , SUM(T2.FREELDFP_USE_AMT       ) AS FREELDFP_USE_AMT        /* 프리LDFPAY사용금액       */
                   , SUM(T2.FREELDFP_USE_CNT       ) AS FREELDFP_USE_CNT        /* 프리LDFPAY사용건수       */
                   , SUM(T2.FREELDFP_OURCOM_BDN_AMT) AS FREELDFP_OURCOM_BDN_AMT /* 프리LDFPAY당사부담금액   */
                   , SUM(T2.FREELDFP_ALYCO_BDN_AMT ) AS FREELDFP_ALYCO_BDN_AMT  /* 프리LDFPAY제휴사부담금액 */
                   , 0  DLR_CROS_ARSE_NSALAMT   /* 달러교차유발순매출액     */
                   , 0  WON_CROS_ARSE_NSALAMT   /* 원화교차유발순매출액     */
                   , 0  OMNI_DLR_NSALAMT        /* 옴니달러순매출액         */
                   , 0  OMNI_WON_NSALAMT        /* 옴니원화순매출액         */
                   , 0  OMNI_DLR_EXCH_NSALAMT   /* 옴니달러교환권순매출액   */
                   , 0  OMNI_WON_EXCH_NSALAMT   /* 옴니원화교환권순매출액   */
                   , SUM(FREELDFP_ACMLT_AMT        ) AS FREELDFP_ACMLT_AMT      /* 프리LDFPAY적립금액 */
                   , SUM(FREELDFP_ACMLT_CNT        ) AS FREELDFP_ACMLT_AMT      /* 프리LDFPAY적립건수 */
                FROM FL_MK_PRMTN_ACTRSLT  T2          /* FL_MK_행사실적 */
               INNER JOIN FL_MK_LGPROMO_CHNG_TEMP T0  /* 변경적재 대상 */
                  ON T2.LGPRCD = T0.LGPRCD
                 AND T2.STR_CD = T0.STR_CD
                 AND T2.INTG_MEMB_NO = T0.INTG_MEMB_NO
               GROUP BY T2.LGPRCD
                      , T2.STR_CD
                      , T2.STD_DT
                      , T2.INTG_CUST_DISTING_NO
                      , T2.INTG_MEMB_NO
                      , T2.CUST_DISTING_NO
                      , T2.CUST_SALES_DVS_CD
                      , T2.SL_CHNL_CD
                      , T2.CUST_CLSF_CD
                      , T2.GRP_CLSF_CD
                      , T2.IMGN_INFO_DVS_CD
                      , T2.IMGN_DT
            ) T1
         GROUP BY LGPRCD
                , STR_CD
                , STD_DT
                , INTG_MEMB_NO
       ) T1
  INNER JOIN D_LGPROMO T5
     ON T1.LGPRCD       = T5.LGPRCD
    AND T5.PRMTN_ACTRSLT_OBJ_XCLUD_YN = 'N'  /* 행사실적대상제외여부 */
   LEFT OUTER JOIN D_MEMB T2
     ON T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
   LEFT OUTER JOIN WL_SL_STR_INFO T3
     ON T1.STR_CD = T3.STR_CD
  INNER JOIN D_INTG_MEMB T6
     ON T1.INTG_MEMB_NO = T6.INTG_MEMB_NO
   LEFT OUTER JOIN WL_SL_PSPT_RCGNT_NO T4
     ON NVL(T6.PSPT_RCGNT_NO, T2.PSPT_RCGNT_NO) = T4.PSPT_RCGNT_NO   /* 여권인식번호 */
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_MERGE_01  *****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

MERGE /*+ PARALLEL(4) */
 INTO FL_MK_LGPROMO_ACTRSLT T1
USING (SELECT T2.LGPRCD
            , T2.STR_CD
            , T2.STD_DT
            , T2.INTG_MEMB_NO
            , T2.DLR_EXCH_NSALAMT
            , T2.WON_EXCH_NSALAMT
         FROM FL_MK_LGPROMO_ACTRSLT T2
        INNER JOIN
            ( SELECT DISTINCT
                     T2.LGPRCD
                   , T3.STR_CD
                   , T1.VIP_NO
                FROM WL_Z_CAMP_CELL_CUST T1
                JOIN D_PRMTN T2
                  ON T1.CAMP_ID            = T2.PRMTNCD
                 AND T2.INTG_MEMB_PRMTN_YN = 'Y'
                JOIN (SELECT PRMTNCD
                           , STR_CD
                           , CHNG_VIP_NO
                           , INTG_MEMB_NO
                        FROM FL_MK_PRMTN_EXCH_CHNG_TEMP
                     ) T3
                  ON T1.CAMP_ID = T3.PRMTNCD
                 AND T1.VIP_NO  = T3.CHNG_VIP_NO
               WHERE NVL(TRIM(T1.SEND_MEMB_CD),T2.CHNL_TYPE_CD ) <> T2.CHNL_TYPE_CD
            ) T3
           ON T2.LGPRCD            = T3.LGPRCD
          AND T2.CUST_DISTING_NO   = T3.VIP_NO
          AND T2.STR_CD            = T3.STR_CD
      ) T2
   ON (T1.LGPRCD       = T2.LGPRCD
  AND  T1.STR_CD       = T2.STR_CD
  AND  T1.STD_DT       = T2.STD_DT
  AND  T1.INTG_MEMB_NO = T2.INTG_MEMB_NO)
 WHEN MATCHED THEN
      UPDATE 
         SET T1.DLR_CROS_EXCH_NSALAMT = NVL(T2.DLR_EXCH_NSALAMT,0)
           , T1.WON_CROS_EXCH_NSALAMT = NVL(T2.WON_EXCH_NSALAMT,0)
           , T1.LOAD_DTTM             = SYSDATE
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;