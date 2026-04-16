/**********  UQ_CREATE_00  ****************************************************/


EXEC PR_CHK_DROP_TABLE('TEMP_OFLN_PRMTN_CHNG');

/* 변경적재모수 */
CREATE TABLE TEMP_OFLN_PRMTN_CHNG NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       DISTINCT PRMTNCD
     , STR_CD
     , INTG_MEMB_NO
     , SYSDATE      AS LOAD_DTTM
  FROM FL_MK_PRMTN_EXCH_CHNG_TEMP T2
 WHERE INTG_MEMB_NO IS NOT NULL
;





/**********  UQ_CREATE_01  ****************************************************/


EXEC PR_CHK_DROP_TABLE('TEMP_PRMTN_ACTRSLT_01');

/* 행사순매출액 계산시 원가 교환권의 상업성구분으로  행사매출과 유발매출의상업매출 제외가능 */

/* 교환권관련 */
CREATE TABLE TEMP_PRMTN_ACTRSLT_01 NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       T1.PRMTNCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.INTG_MEMB_NO
     , MIN(T1.CUST_SALES_DVS_CD)                      AS CUST_SALES_DVS_CD
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN NVL(T3.DLR_PDCST,0)*T1.SALES_SIGN ELSE 0 END)         AS DLR_PDCST_SUM_AMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN NVL(T3.WON_PDCST,0)*T1.SALES_SIGN ELSE 0 END)         AS WON_PDCST_SUM_AMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS DLR_EXCH_NSALAMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS WON_EXCH_NSALAMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.SALES_SIGN ELSE 0 END) AS EXCH_CNT
     , SUM(ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0)) AS DLR_PRMTN_NSALAMT
     , SUM(ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0)) AS WON_PRMTN_NSALAMT
     , SUM(T1.SALES_SIGN) AS PRMTN_EXCH_CNT
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_DLR_NSALAMT    /* 옴니달러순매출액 */
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_WON_NSALAMT    /* 옴니원화순매출액 */
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' AND EXCH_PRMTN_APLY_YN ='Y' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_DLR_EXCH_NSALAMT  /* 옴니달러교환권순매출액 */
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' AND EXCH_PRMTN_APLY_YN ='Y' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_WON_EXCH_NSALAMT  /* 옴니원화교환권순매출액 */
     , SUM(T1.DLR_ARSE_NSALAMT*T1.SALES_SIGN) AS DLR_ARSE_NSALAMT        /* 달러유발순매출액        */
     , SUM(T1.WON_ARSE_NSALAMT*T1.SALES_SIGN) AS WON_ARSE_NSALAMT        /* 원화유발순매출액        */
     , SUM(CASE WHEN T1.SL_CHNL_CD IN ('4','5') THEN T1.DLR_ARSE_NSALAMT*T1.SALES_SIGN ELSE 0 END) AS DLR_CROS_ARSE_NSALAMT   /* 달러교차유발순매출액     */
     , SUM(CASE WHEN T1.SL_CHNL_CD IN ('4','5') THEN T1.WON_ARSE_NSALAMT*T1.SALES_SIGN ELSE 0 END) AS WON_CROS_ARSE_NSALAMT   /* 원화교차유발순매출액     */
  FROM FL_MK_PRMTN_EXCH_SL T1         /* FL_MK_행사교환권판매 */
 INNER JOIN TEMP_OFLN_PRMTN_CHNG T2   /* 변경적재 대상 */
    ON T1.PRMTNCD      = T2.PRMTNCD
   AND T1.STR_CD       = T2.STR_CD
   AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
  LEFT OUTER JOIN
     (  /* 원가금액 가져오기 */
       SELECT /*+ PARALLEL(4) */
              T1.STR_CD
            , T1.SL_DT
            , T1.EXCH_NO
            , SUM(T1.DLR_PDCST*T1.SL_QTY)  AS DLR_PDCST
            , SUM(T1.WON_PDCST*T1.SL_QTY)  AS WON_PDCST
         FROM FL_SL_PROD_SL T1 /* FL_SL_상품판매 */
        INNER JOIN ( /*  행사 관련 적재 대상 고객의 전체 교환권 */
              SELECT DISTINCT T1.STD_DT
                   , T1.STR_CD
                   , T1.EXCH_NO
                FROM FL_MK_PRMTN_EXCH_SL T1  /* FL_MK_행사교환권판매 */
               INNER JOIN (
                     SELECT DISTINCT T2.PRMTNCD 
                          , T1.STR_CD
                          , T1.INTG_MEMB_NO
                       FROM FL_MK_PRMTN_EXCH_CHNG_TEMP t1
                      INNER JOIN D_PRMTN T2
                         ON T1.PRMTNCD = T2.PRMTNCD
                    ) T2
                  ON T1.PRMTNCD       = T2.PRMTNCD
                 AND T1.STR_CD       = T2.STR_CD
                 AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
             )   T2 
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
 GROUP BY T1.PRMTNCD
        , T1.STR_CD
        , T1.STD_DT
        , T1.INTG_MEMB_NO
;






/**********  UQ_CREATE_02  ****************************************************/


EXEC PR_CHK_DROP_TABLE('TEMP_PRMTN_ACTRSLT_02');

/* 할인금액 */
CREATE TABLE TEMP_PRMTN_ACTRSLT_02 NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       T1.PRMTNCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.INTG_MEMB_NO
     , MIN(T1.CUST_SALES_DVS_CD)                   AS CUST_SALES_DVS_CD  /* 1:회원매출, 2:비회원매출, 3:미인식매출 */
     , SUM(CASE WHEN T2.OURCOM_BDNRT IS NOT NULL THEN (T5.WON_DC_AMT*T2.OURCOM_BDNRT/100.0)*T1.SALES_SIGN ELSE (T5.WON_DC_AMT*NVL(T3.OURCOM_BDNRT,100)/100.0)*T1.SALES_SIGN END)      AS DC_OURCOM_BDN_AMT
     , SUM(CASE WHEN T2.OURCOM_BDNRT IS NOT NULL THEN (T5.WON_DC_AMT*(100.0-T2.OURCOM_BDNRT)/100.0)*T1.SALES_SIGN ELSE (T5.WON_DC_AMT*NVL(T3.ALYCO_BDNRT,0)/100.0)*T1.SALES_SIGN END) AS DC_ALYCO_BDN_AMT
     , SUM(ROUND(T5.DLR_DC_AMT*T1.SALES_SIGN,0))  AS DLR_PRMTN_DC_AMT
     , SUM(ROUND(T5.WON_DC_AMT*T1.SALES_SIGN,0))  AS WON_PRMTN_DC_AMT
     , SUM(T1.SALES_SIGN)                          AS DC_CNT          /* 할인건수  */
     , SUM(T4.WON_TOT_DC_AMT)                      AS WON_TOT_DC_AMT
  FROM FL_MK_PRMTN_EXCH_SL T1        /* FL_MK_행사교환권판매 */
 INNER JOIN TEMP_OFLN_PRMTN_CHNG T0  /* 변경적재 대상 */
    ON T1.PRMTNCD      = T0.PRMTNCD
   AND T1.STR_CD       = T0.STR_CD
   AND T1.INTG_MEMB_NO = T0.INTG_MEMB_NO
 INNER JOIN WL_SL_SALE_HDR_DC T5
    ON T1.PRMTNCD = T5.PRMTNCD
   AND T1.STR_CD = T5.STR_CD
   AND T1.STD_DT = T5.SL_DT
   AND T1.EXCH_NO = T5.EXCH_NO
 INNER JOIN D_PRMTN T2  /* D_행사 */
    ON T1.PRMTNCD         = T2.PRMTNCD
   AND T1.PRMTN_LGCSF_CD  = '001'   /* 할인 */
  LEFT OUTER JOIN WL_LC_PRMTN_OFFER_SECT T3  /* WL_LC_행사OFFER구간 */
    ON T1.CMPN_OFFER_NO   = T3.CMPN_OFFER_NO
   AND T1.PRMTN_SECTRG_NO = T3.PRMTN_SECTRG_NO
 INNER JOIN (
        SELECT /*+ PARALLEL(4) */
               T1.STD_DT
             , T1.STR_CD
             , T1.INTG_MEMB_NO
             , T1.CUST_SALES_DVS_CD
             , SUM(T1.WON_TOT_DC_AMT*T1.SALES_SIGN)
               - SUM(CASE WHEN T2.OURCOM_BDNRT IS NOT NULL THEN (T1.WON_TOT_DC_AMT*(100.0-T2.OURCOM_BDNRT)/100.0)*T1.SALES_SIGN ELSE (T1.WON_TOT_DC_AMT*NVL(T3.ALYCO_BDNRT,0)/100.0)*T1.SALES_SIGN END) AS WON_TOT_DC_AMT
          FROM FL_MK_PRMTN_EXCH_SL T1         /* FL_MK_행사교환권판매 */
         INNER JOIN TEMP_OFLN_PRMTN_CHNG T0   /* 변경적재 대상 */
            ON T1.PRMTNCD      = T0.PRMTNCD
           AND T1.STR_CD       = T0.STR_CD
           AND T1.INTG_MEMB_NO = T0.INTG_MEMB_NO
         INNER JOIN D_PRMTN T2 /* D_행사 */
            ON T1.PRMTNCD      = T2.PRMTNCD
          LEFT OUTER JOIN WL_LC_PRMTN_OFFER_SECT T3 /* WL_LC_행사OFFER구간 */
            ON T1.CMPN_OFFER_NO   = T3.CMPN_OFFER_NO
           AND T1.PRMTN_SECTRG_NO = T3.PRMTN_SECTRG_NO
         WHERE T1.PRMTN_LGCSF_CD  = '001'  /* 할인 */
         GROUP BY T1.STD_DT
                , T1.STR_CD
                , T1.INTG_MEMB_NO
                , T1.CUST_SALES_DVS_CD
        ) T4
    ON T1.STR_CD            = T4.STR_CD
   AND T1.STD_DT            = T4.STD_DT
   AND T1.INTG_MEMB_NO      = T4.INTG_MEMB_NO
   AND T1.CUST_SALES_DVS_CD = T4.CUST_SALES_DVS_CD
 GROUP BY T1.PRMTNCD
        , T1.STR_CD
        , T1.STD_DT
        , T1.INTG_MEMB_NO
;





/**********  UQ_CREATE_03  ****************************************************/


EXEC PR_CHK_DROP_TABLE('TEMP_PRMTN_ACTRSLT_03');

/* 일반사은품 증정 */
CREATE TABLE TEMP_PRMTN_ACTRSLT_03 NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       A01.PRMTNCD
     , A01.STR_CD
     , A01.STD_DT
     , A01.INTG_MEMB_NO
     , A01.CUST_SALES_DVS_CD
     , NVL(A01.FGF_PRESTAT_AMT,0)            AS FGF_PRESTAT_AMT          /* 사은품증정금액   */
     , NVL(A01.FGF_PRESTAT_CNT,0)            AS FGF_PRESTAT_CNT          /* 사은품증정건수   */
     , NVL(A01.OURCOM_BDN_AMT,0)             AS PRESTAT_OURCOM_BDN_AMT   /* 당사부담금액     */
     , NVL(A01.ALYCO_BDN_AMT,0)              AS PRESTAT_ALYCO_BDN_AMT    /* 제휴사부담금액   */
     , NVL(A02.TOT_PRESTAT_AMT,0)            AS TOT_PRESTAT_AMT          /* 총증정금액       */
     , NVL(A01.PRMTN_PRESTAT_AMT,0)          AS PRMTN_PRESTAT_AMT        /* 행사증정금액     */
     , NVL(A01.PRMTN_CASH_RTRV_AMT,0)        AS PRMTN_CASH_RTRV_AMT      /* 행사현금회수금액 */
  FROM (
       SELECT /*+ USE_HASH(T1 T2) PARALLEL(4) */
              T1.PRMTNCD
            , T1.STD_DT
            , T1.STR_CD
            , T1.INTG_MEMB_NO
            , MIN(T2.CUST_SALES_DVS_CD) AS CUST_SALES_DVS_CD
            , SUM( CASE WHEN T4.PRMTN_MDCSF_CD = '03001' THEN
                        DECODE(T2.PRESTAT_DVS_CD,'0',1,-1) *
                            (CASE WHEN NVL(T2.CASH_RTRV_AMT,0) = 0 THEN (
                                  CASE WHEN ( T2.PRESTAT_DVS_CD = '1' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T2.FGF_RTRV_METHD_CD ='5' )  THEN 0  /* 기타회수 */
                                       WHEN ( T2.PRESTAT_DVS_CD = '0' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T10.PRESTAT_SN IS NOT NULL ) THEN 0  /* 증정의 원증정일련번호의 사은품회수방법코드가 '5' 기타회수면 당사부당금은 0 */
                                       ELSE NVL(T3.FGF_UPRC,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0) END )
                             WHEN NVL(T2.CASH_RTRV_AMT,0) <> 0 THEN ( NVL(T2.CASH_RTRV_AMT,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0) )
                        END )
                   ELSE 0 END ) AS FGF_PRESTAT_AMT                 /* 사은품증정금액     */
            , COUNT(DISTINCT (CASE WHEN T4.PRMTN_MDCSF_CD = '03001' AND T2.PRESTAT_DVS_CD = '0' THEN T2.PRESTAT_SN ELSE NULL END))
              -COUNT(DISTINCT (CASE WHEN T4.PRMTN_MDCSF_CD = '03001' AND T2.PRESTAT_DVS_CD = '1' THEN T2.PRESTAT_SN ELSE NULL END))  AS FGF_PRESTAT_CNT /* 사은품증정건수 */
            , SUM( DECODE(T2.PRESTAT_DVS_CD,'0',1,-1) *
                  (CASE WHEN NVL(T2.CASH_RTRV_AMT,0) = 0 THEN (
                             CASE WHEN ( T2.PRESTAT_DVS_CD = '1' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T2.FGF_RTRV_METHD_CD ='5' ) THEN 0   /* 기타회수 */
                                  WHEN ( T2.PRESTAT_DVS_CD = '0' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T10.PRESTAT_SN IS NOT NULL ) THEN 0  /* 증정의 원증정일련번호의 사은품회수방법코드가 '5' 기타회수면 당사부당금은 0 */
                                  ELSE NVL(T3.FGF_UPRC,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0) END )
                        WHEN NVL(T2.CASH_RTRV_AMT,0) <> 0 THEN ( NVL(T2.CASH_RTRV_AMT,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0) )
                   END ) )   AS OURCOM_BDN_AMT                  /* 당사부담금액    */
            , SUM(DECODE(T2.PRESTAT_DVS_CD,'0',1,-1) * NVL(ABS(T2.ALYCO_BDN_AMT),0)) AS ALYCO_BDN_AMT           /* 제휴사부담금액 : 회수시는 항상 - 금액이 되도록 */
            , SUM(DECODE(T2.PRESTAT_DVS_CD,'0',1,-1) * NVL(T2.PRESTAT_AMT,0))        AS PRMTN_PRESTAT_AMT       /* 행사증정금액*/
            , SUM(DECODE(T2.PRESTAT_DVS_CD,'0',1,-1) * NVL(T2.CASH_RTRV_AMT,0))      AS PRMTN_CASH_RTRV_AMT     /* 행사현금회수금액*/
         FROM (
               SELECT /*+ PARALLEL(4) */
                      T1.PRMTNCD
                    , T1.STD_DT
                    , T1.STR_CD
                    , T1.INTG_MEMB_NO
                 FROM FL_MK_PRMTN_EXCH_CHNG_TEMP T1
                GROUP BY T1.PRMTNCD
                       , T1.STD_DT
                       , T1.STR_CD
                       , T1.INTG_MEMB_NO
            ) T1
        INNER JOIN FL_MK_PRMTN_FGF_PRESTAT_DTL T2  /* FL_MK_행사사은품증정상세 */
           ON T1.PRMTNCD = T2.PRMTNCD
          AND T1.STR_CD = T2.STR_CD
          AND T1.STD_DT = T2.PYMT_STD_DT
          AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
        INNER JOIN D_PRMTN T4  /* D_행사 */
           ON T1.PRMTNCD = T4.PRMTNCD
         LEFT OUTER JOIN WL_MK_FGF_BASIC_INFO T3 /* WL_MK_사은품기본정보 */
           ON T2.FGFCD = T3.FGFCD
         LEFT OUTER JOIN WL_MK_PRMTN_FGF_PRESTAT_DTL T10  /* WL_MK_행사사은품증정상세 */
           ON T2.ORIG_PRESTAT_SN = T10.PRESTAT_SN
          AND T10.FGF_RTRV_METHD_CD = '5'    /* 기타회수 당사부담금 0원 (기타회수의 경우 사은품금액이 제대로 안들어옴) */
        WHERE T4.PRMTN_LGCSF_CD     = '003'  /* 사은품 */
        GROUP BY T1.PRMTNCD
               , T1.STD_DT
               , T1.STR_CD
               , T1.INTG_MEMB_NO
       HAVING SUM(CASE WHEN T2.PRESTAT_DVS_CD = '0' THEN 1 ELSE -1 END)  <> 0
      ) A01
  INNER JOIN (
       SELECT /*+ USE_HASH(T1 T2) PARALLEL(4)*/
              T1.STD_DT
            , T1.STR_CD
            , T1.INTG_MEMB_NO
            , MIN(T2.CUST_SALES_DVS_CD) AS CUST_SALES_DVS_CD
            , SUM( DECODE(T2.PRESTAT_DVS_CD,'0',1,-1) * (
                        CASE WHEN NVL(T2.CASH_RTRV_AMT,0) = 0 THEN (
                             CASE WHEN ( T2.PRESTAT_DVS_CD = '1' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T2.FGF_RTRV_METHD_CD ='5' ) THEN 0   /* 기타회수 */
                                  WHEN ( T2.PRESTAT_DVS_CD = '0' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T10.PRESTAT_SN IS NOT NULL ) THEN 0  /* 증정의 원증정일련번호의 사은품회수방법코드가 '5' 기타회수면 당사부당금은 0 */
                                 ELSE NVL(T3.FGF_UPRC,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0) END )
                             WHEN NVL(T2.CASH_RTRV_AMT,0) <> 0 THEN (NVL(T2.CASH_RTRV_AMT,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0))
                         END ) )  AS TOT_PRESTAT_AMT
         FROM (
               SELECT /*+  PARALLEL(4) */
                      T0.PRMTNCD
                    , T0.STR_CD
                    , T0.INTG_MEMB_NO
                    , T0.STD_DT
                 FROM FL_MK_PRMTN_EXCH_CHNG_TEMP T0         /* FL_MK_행사교환권판매 */
                GROUP BY T0.PRMTNCD
                       , T0.STR_CD
                       , T0.INTG_MEMB_NO
                       , T0.STD_DT
            ) T1
        INNER JOIN FL_MK_PRMTN_FGF_PRESTAT_DTL T2  /* FL_MK_행사사은품증정상세 */
           ON T1.PRMTNCD = T2.PRMTNCD
          AND T1.STR_CD  = T2.STR_CD
          AND T1.STD_DT  = T2.PYMT_STD_DT
          AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
        INNER JOIN D_PRMTN T4  /* D_행사 */
           ON T1.PRMTNCD = T4.PRMTNCD
         LEFT OUTER JOIN WL_MK_FGF_BASIC_INFO T3 /* WL_MK_사은품기본정보 */
           ON T2.FGFCD   = T3.FGFCD
         LEFT OUTER JOIN WL_MK_PRMTN_FGF_PRESTAT_DTL T10  /* WL_MK_행사사은품증정상세 */
           ON T2.ORIG_PRESTAT_SN = T10.PRESTAT_SN
          AND T10.FGF_RTRV_METHD_CD = '5'    /* 기타회수 당사부담금 0원 (기타회수의 경우 사은품금액이 제대로 안들어옴) */
        WHERE T4.PRMTN_LGCSF_CD     = '003'  /* 사은품 */
       GROUP BY T1.STD_DT
              , T1.STR_CD
              , T1.INTG_MEMB_NO
        HAVING SUM(CASE WHEN T2.PRESTAT_DVS_CD = '0' THEN 1 ELSE -1 END)  <> 0
      ) A02
    ON A01.STD_DT       = A02.STD_DT
   AND A01.STR_CD       = A02.STR_CD
   AND A01.INTG_MEMB_NO = A02.INTG_MEMB_NO
;





/**********  UQ_CREATE_04  ****************************************************/


EXEC PR_CHK_DROP_TABLE('TEMP_PRMTN_ACTRSLT_04');

/* LDFPAY 증정 */
CREATE TABLE TEMP_PRMTN_ACTRSLT_04 NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       K0.PRMTNCD
     , K1.PYMT_STD_DT AS STD_DT
     , K0.STR_CD
     , K0.INTG_MEMB_NO
     , MIN(CASE WHEN K1.VIP_NO IS NOT NULL THEN '1' ELSE '2' END) AS CUST_SALES_DVS_CD
     , SUM(CASE WHEN K3.PRMTN_MDCSF_CD = '05003' THEN 0
                ELSE CASE WHEN K1.PRESTAT_DVS_CD = '0' THEN K2.PRESTAT_AMT ELSE -K2.PRESTAT_AMT END END) AS LDFP_ACMLT_AMT
     , SUM(CASE WHEN K3.PRMTN_MDCSF_CD = '05003' THEN CASE WHEN K1.PRESTAT_DVS_CD = '0' THEN K2.PRESTAT_AMT ELSE -K2.PRESTAT_AMT END
                ELSE 0 END)                                                                              AS FREELDFP_ACMLT_AMT
     , COUNT(DISTINCT CASE WHEN K3.PRMTN_MDCSF_CD <> '05003' AND K1.PRESTAT_DVS_CD = '0' THEN K1.PRESTAT_SN END)
                                    -NVL(COUNT(DISTINCT CASE WHEN K3.PRMTN_MDCSF_CD <> '05003' AND K1.PRESTAT_DVS_CD = '1' THEN K1.PRESTAT_SN END),0)       AS LDFP_ACMLT_CNT
     , COUNT(DISTINCT CASE WHEN K3.PRMTN_MDCSF_CD = '05003' AND K1.PRESTAT_DVS_CD = '0' THEN K1.PRESTAT_SN END)
                                    -NVL(COUNT(DISTINCT CASE WHEN K3.PRMTN_MDCSF_CD = '05003' AND K1.PRESTAT_DVS_CD = '1' THEN K1.PRESTAT_SN END),0)       AS FREELDFP_ACMLT_CNT
     , SUM(CASE WHEN K3.PRMTN_MDCSF_CD = '05003' THEN 0 ELSE NVL(K6.LDFP_OURCOM_BDN_AMT,DECODE(K1.PRESTAT_DVS_CD, '0', 1, -1)*K2.PRESTAT_AMT*NVL(K5.OURCOM_BDNRT,100)/100) END)  AS LDFP_OURCOM_BDN_AMT
     , SUM(CASE WHEN K3.PRMTN_MDCSF_CD = '05003' THEN 0 ELSE NVL(K6.LDFP_ALYCO_BDN_AMT,DECODE(K1.PRESTAT_DVS_CD, '0', 1, -1)*K2.PRESTAT_AMT*NVL(K5.ALYCO_BDNRT,0)/100) END)      AS LDFP_ALYCO_BDN_AMT
  FROM TEMP_OFLN_PRMTN_CHNG        K0 /* 변경모수 */
 INNER JOIN WL_MK_PRMTN_FGF_PRESTAT K1
    ON K0.STR_CD         = K1.STR_CD
   AND K0.PRMTNCD        = K1.PRMTNCD
   AND K0.INTG_MEMB_NO   = K1.INTG_MEMB_NO
 INNER JOIN D_PRMTN K3  /* D_행사 */
    ON K3.PRMTNCD = K1.PRMTNCD
   AND K1.PRESTAT_DT  BETWEEN K3.PRMTN_STRT_DT AND K3.PRMTN_END_DT
 INNER JOIN  WL_MK_PRMTN_FGF_PRESTAT_DTL K2  /* WL_MK_행사사은품증정상세 */
    ON K1.STR_CD            = K2.STR_CD
   AND K1.PRMTNCD           = K2.PRMTNCD
   AND K1.PRESTAT_DVS_CD    = K2.PRESTAT_DVS_CD
   AND K1.PRESTAT_SN        = K2.PRESTAT_SN
 INNER JOIN D_LDFP K4  /* D_LDFPAY */
    ON K2.LDFP_NO = K4.LDFP_NO
  LEFT OUTER JOIN WL_LC_PRMTN_OFFER_SECT_LDFP K5 /* WL_LC_행사OFFER구간LDFPAY */
    ON K4.CMPN_OFFER_NO   = K5.CMPN_OFFER_NO
   AND K4.PRMTN_SECTRG_NO = K5.PRMTN_SECTRG_NO
   AND K4.SEQ             = K5.SEQ
  LEFT OUTER JOIN (
       SELECT LDFP_HIST_HAPN_DT
            , INTG_MEMB_NO
            , STR_CD
            , LDFP_NO
            , SUM(LDFP_OURCOM_BDN_AMT) AS LDFP_OURCOM_BDN_AMT
            , SUM(LDFP_ALYCO_BDN_AMT)  AS LDFP_ALYCO_BDN_AMT
         FROM WL_LP_LDFP_NO_ACMLT_USE_CAP_MGT  /* WL_LP_LDFPAY번호적립사용CAP관리 */
        WHERE LDFP_ACMLT_YN = 'Y'
        GROUP BY LDFP_HIST_HAPN_DT
               , INTG_MEMB_NO
               , STR_CD
               , LDFP_NO
      ) K6
    ON K1.PRESTAT_DT   = K6.LDFP_HIST_HAPN_DT
   AND K1.INTG_MEMB_NO = K6.INTG_MEMB_NO
   AND K4.LDFP_NO      = K6.LDFP_NO
   AND K1.STR_CD       = K6.STR_CD
 GROUP BY K0.PRMTNCD
        , K1.PYMT_STD_DT
        , K0.STR_CD
        , K0.INTG_MEMB_NO
HAVING SUM(CASE WHEN K1.PRESTAT_DVS_CD = '0' THEN 1 ELSE -1 END)  <> 0
;





/**********  UQ_CREATE_05  ****************************************************/


EXEC PR_CHK_DROP_TABLE('TEMP_PRMTN_ACTRSLT_05');

CREATE TABLE TEMP_PRMTN_ACTRSLT_05 NOLOGGING AS
/* 프리 LDFPAY 사용 */
SELECT /*+ USE_HASH(T1 T3 T7 T4) PARALLEL(4) */
       T1.PRMTNCD
     , T1.STD_DT
     , T1.STR_CD
     , T1.INTG_MEMB_NO
     , MIN(CASE WHEN T7.VIP_NO IS NOT NULL THEN '1' ELSE '2' END) AS CUST_SALES_DVS_CD
     , SUM(CASE WHEN T3.LDFP_RCPDSBS_TYPE_CD in('21','26') THEN T3.LDFP_USE_AMT ELSE 0 END)  AS FREELDFP_USE_AMT
     , COUNT(DISTINCT CASE WHEN T3.LDFP_RCPDSBS_TYPE_CD = '21' THEN T3.LDFP_TRX_APRVNO END)
                        -NVL(COUNT(DISTINCT CASE WHEN T3.LDFP_RCPDSBS_TYPE_CD = '26' THEN T3.LDFP_TRX_APRVNO END),0)   AS FREELDFP_USE_CNT
     , SUM(CASE WHEN T3.LDFP_RCPDSBS_TYPE_CD in('21','26') THEN NVL(T6.LDFP_OURCOM_BDN_AMT,T3.LDFP_USE_AMT*NVL(T5.OURCOM_BDNRT,100)/100) ELSE 0 END )  AS FREELDFP_OURCOM_BDN_AMT
     , SUM(CASE WHEN T3.LDFP_RCPDSBS_TYPE_CD in('21','26') THEN NVL(T6.LDFP_ALYCO_BDN_AMT,T3.LDFP_USE_AMT*NVL(T5.ALYCO_BDNRT,0)/100) ELSE 0 END )  AS FREELDFP_ALYCO_BDN_AMT
  FROM
     (
        SELECT /*+  PARALLEL(4) */
               T1.PRMTNCD
             , T1.STD_DT
             , T1.STR_CD
             , T1.INTG_MEMB_NO
          FROM FL_MK_PRMTN_EXCH_CHNG_TEMP T1         /* FL_MK_행사교환권판매 */
         GROUP BY T1.PRMTNCD
                , T1.STD_DT
                , T1.STR_CD
                , T1.INTG_MEMB_NO
     ) T1
 INNER JOIN FL_MK_LDFP_ACMLT_USE_PTCLS T3  /* 해당테이블에 ACMLT_USE_DVS_CD= '2'(사용)인 경우 LRWD_NO별로 사용,사용취소가 집계되어 있음 */
    ON T1.PRMTNCD      = T3.PRMTNCD
   AND T1.STD_DT       = T3.LDFP_HIST_HAPN_DT
   AND T1.INTG_MEMB_NO = T3.INTG_MEMB_NO
   AND T3.LDFP_RCPDSBS_TYPE_CD IN ('21','26')  /* 사용, 사용취소 */
 INNER JOIN D_INTG_MEMB T7
    ON T3.INTG_MEMB_NO = T7.INTG_MEMB_NO
 INNER JOIN D_PRMTN T4   /* D_행사 */
    ON T1.PRMTNCD = T4.PRMTNCD
   AND T1.STD_DT BETWEEN T4.PRMTN_STRT_DT AND T4.PRMTN_END_DT
   AND T4.PRMTN_LGCSF_CD = '005'    /* LDFPAY */
   AND T4.PRMTN_MDCSF_CD = '05003'  /* 프리LDFPAY */
 INNER JOIN D_LDFP T2  /* D_LDFPAY */
    ON T3.LDFP_NO  = T2.LDFP_NO
 INNER JOIN WL_LC_PRMTN_OFFER_SECT_LDFP T5  /* WL_LC_행사OFFER구간LDFPAY */
    ON T2.CMPN_OFFER_NO     = T5.CMPN_OFFER_NO
   AND T2.PRMTN_SECTRG_NO   = T5.PRMTN_SECTRG_NO
   AND T2.SEQ               = T5.SEQ
  LEFT OUTER JOIN WL_LP_LDFP_NO_ACMLT_USE_CAP_MGT T6 /* WL_LP_LDFPAY번호적립사용CAP관리 */
    ON T3.LDFP_HIST_HAPN_DT = T6.LDFP_HIST_HAPN_DT
   AND T3.INTG_MEMB_NO      = T6.INTG_MEMB_NO
   AND T3.LDFP_NO           = T6.LDFP_NO
   AND T3.STR_CD            = T6.STR_CD
   AND T3.SEQ               = T6.SEQ
   AND T3.HIST_SEQ          = T6.HIST_SEQ
   AND T6.LDFP_ACMLT_YN     = 'Y'
 GROUP BY T1.PRMTNCD
        , T1.STD_DT
        , T1.STR_CD
        , T1.INTG_MEMB_NO
 HAVING SUM(CASE WHEN T3.LDFP_RCPDSBS_TYPE_CD in('21','26') THEN T3.LDFP_USE_AMT ELSE 0 END) <> 0
;





/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FL_MK_PRMTN_ACTRSLT T1
 WHERE EXISTS (
               SELECT 1
                 FROM TEMP_OFLN_PRMTN_CHNG T0  /* 변경적재 대상 */
                WHERE T1.STR_CD       = T0.STR_CD
                  AND T1.PRMTNCD      = T0.PRMTNCD
                  AND T1.INTG_MEMB_NO = T0.INTG_MEMB_NO
              );

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FL_MK_PRMTN_ACTRSLT T
SELECT /*+ PARALLEL(4) */
       T1.PRMTNCD                 /* 행사코드             */
     , T1.STR_CD                  /* 점코드               */
     , T1.STD_DT                  /* 기준일자             */
     , T1.INTG_MEMB_NO            /* 통합회원번호           */
     , T1.INTG_CUST_DISTING_NO
     , CASE WHEN T1.INTG_MEMB_NO IS NOT NULL THEN 'Y' ELSE 'N' END AS INTG_MEMB_YN
     , T1.CUST_DISTING_NO         /* 고객식별번호           */
     --, CASE WHEN T1.CUST_SALES_DVS_CD = '1' THEN T2.PSPT_RCGNT_NO
     --       WHEN T1.CUST_SALES_DVS_CD = '2' THEN TO_NUMBER(TRIM(SUBSTR(T1.CUST_DISTING_NO,4,9) ))
     --       ELSE  0   END         AS PSPT_RCGNT_NO  /* 여권인식번호           */
	 , NVL(T6.PSPT_RCGNT_NO, T2.PSPT_RCGNT_NO)  AS PSPT_RCGNT_NO             /* 여권인식번호           */
     /*********
     , CASE WHEN TRANSLATE(SUBSTR(T4.PSPT_SHTN_NO,1,4),'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ','999999999999999999999999999999999999') =  '9999'
            THEN 'Y' ELSE 'N' END PSPTNO_NML_YN
     *****/
     , CASE WHEN REGEXP_COUNT(SUBSTR(T4.PSPT_SHTN_NO,1,4),'[0-9|A-Z]') = 4 THEN 'Y'
            ELSE 'N'
       END                                AS PSPTNO_NML_YN          /* 여권번호정상여부       */
     , NVL(T1.CUST_SALES_DVS_CD     ,'z') AS CUST_SALES_DVS_CD      /* 고객매출구분코드       */
     , NVL(T1.SL_CHNL_CD,'z')             AS SL_CHNL_CD             /* 판매채널코드           */
     , NVL(T2.MEMB_DVS_CD           ,'z') AS MEMB_DVS_CD            /* 회원구분코드           */
     , 'z'                                AS VIP_CARD_DVS_CD        /* VIP카드구분코드        */
    -- , CASE WHEN T2.VIP_NO IS NOT NULL THEN DECODE(T2.NATLT_CD,'z','KOR',T2.NATLT_CD)
    --            WHEN T1.CUST_SALES_DVS_CD = '2' THEN SUBSTR(T1.CUST_DISTING_NO,1,3)
    --            ELSE 'KOR'  END           AS NATLT_CD               /* 국적코드               */
	 , NVL(T6.NATLT_CD,'z')               AS NATLT_CD               /* 국적코드            */
     , NVL(T2.NTV_FORN_DVS_CD       ,'z') AS NTV_FORN_DVS_CD        /* 내국인외국인구분코드   */
     , NVL(T2.RESID_STD_NATLT_DVS_CD,'z') AS RESID_STD_NATLT_DVS_CD /* 거주기준국적구분코드   */
     , NVL(T6.SEX_CD                ,'z') AS SEX_CD                 /* 성별코드               */
     , NVL(T6.AGE_CD                ,999) AS AGE_CD                 /* 연령코드               */
     ,  'z'                               AS FRNCTR_RGN_DVS_CD      /* 외국지역구분코드       */
     , NVL(T6.INTG_MEMB_GRD_CD      ,'z') AS INTG_MEMB_GRD_CD       /* 통합회원등급코드       */
     , NVL(T2.WDRMBSHIP_YN          ,'z') AS WDRMBSHIP_YN           /* 탈회여부               */
     , CASE WHEN T1.CUST_DISTING_NO IS NOT NULL THEN 'Y' ELSE 'N' END AS PRMTN_TGTPSN_YN      /* 행사대상자여부         */
     , T5.LGPRCD                                                    /* 대행사코드             */
     , T5.MDPRCD                                                    /* 중행사코드             */
     , T5.PRMTN_LGCSF_CD                                            /* 행사대분류코드         */
     , T5.PRMTN_MDCSF_CD                                            /* 행사중분류코드         */
     , T5.CMPN_OFFER_NO                                             /* 캠페인OFFER번호        */
     , NVL(T1.CUST_CLSF_CD         ,'z') AS CUST_CLSF_CD
     , NVL(T1.GRP_CLSF_CD          ,'z') AS GRP_CLSF_CD
     , NVL(T1.IMGN_INFO_DVS_CD     ,'z') AS IMGN_INFO_DVS_CD        /* 출입국정보구분코드*/
     , T1.IMGN_DT                                                   /* 출입국일자 */
     , NVL(T1.DLR_PDCST_SUM_AMT      ,0) AS DLR_PDCST_SUM_AMT       /* 달러원가합계금액       */
     , NVL(T1.WON_PDCST_SUM_AMT      ,0) AS WON_PDCST_SUM_AMT       /* 원화원가합계금액       */
     , NVL(T1.DLR_EXCH_NSALAMT       ,0) AS DLR_EXCH_NSALAMT        /* 달러교환권순매출액     */
     , NVL(T1.WON_EXCH_NSALAMT       ,0) AS WON_EXCH_NSALAMT        /* 원화교환권순매출액     */
     , NVL(T1.EXCH_CNT               ,0) AS EXCH_CNT                /* 교환권건수             */
     , NVL(T1.DLR_PRMTN_NSALAMT      ,0) AS DLR_PRMTN_NSALAMT       /* 달러행사순매출액       */
     , NVL(T1.WON_PRMTN_NSALAMT      ,0) AS WON_PRMTN_NSALAMT       /* 원화행사순매출액       */
     , NVL(T1.PRMTN_EXCH_CNT         ,0) AS PRMTN_EXCH_CNT          /* 행사교환권건수         */
     , NVL(T1.DLR_ARSE_NSALAMT       ,0) AS DLR_ARSE_NSALAMT        /* 달러유발순매출액       */
     , NVL(T1.WON_ARSE_NSALAMT       ,0) AS WON_ARSE_NSALAMT        /* 원화유발순매출액       */
     , NVL(T1.DLR_PRMTN_DC_AMT       ,0) AS DLR_PRMTN_DC_AMT        /* 달러행사할인금액       */
     , NVL(T1.WON_PRMTN_DC_AMT       ,0) AS WON_PRMTN_DC_AMT        /* 원화행사할인금액       */
     , NVL(T1.DC_OURCOM_BDN_AMT      ,0) AS DC_OURCOM_BDN_AMT       /* 할인당사부담금액       */
     , NVL(T1.DC_ALYCO_BDN_AMT       ,0) AS DC_ALYCO_BDN_AMT        /* 할인제휴사부담금액     */
     , NVL(T1.DC_CNT                 ,0) AS DC_CNT                  /* 할인건수               */
     , MAX(NVL(T1.WON_TOT_DC_AMT,0)) OVER (PARTITION BY T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO, T1.CUST_DISTING_NO) AS WON_TOT_DC_AMT          /* 원화총할인금액         */
     , NVL(T1.FGF_PRESTAT_AMT        ,0) AS FGF_PRESTAT_AMT         /* 사은품증정금액         */
     , NVL(T1.FGF_PRESTAT_CNT        ,0) AS FGF_PRESTAT_CNT         /* 사은품증정건수         */
     , NVL(T1.PRESTAT_OURCOM_BDN_AMT ,0) AS PRESTAT_OURCOM_BDN_AMT  /* 증정당사부담금액       */
     , NVL(T1.PRESTAT_ALYCO_BDN_AMT  ,0) AS PRESTAT_ALYCO_BDN_AMT   /* 증정제휴사부담금액     */
     , MAX(NVL(T1.TOT_PRESTAT_AMT,0)) OVER (PARTITION BY T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO, T1.CUST_DISTING_NO) AS TOT_PRESTAT_AMT         /* 총증정금액            */
     , NVL(T1.PRMTN_PRESTAT_AMT      ,0) AS PRMTN_PRESTAT_AMT       /* 행사증정금액             */
     , NVL(T1.PRMTN_CASH_RTRV_AMT    ,0) AS PRMTN_CASH_RTRV_AMT     /* 행사현금회수금액         */
     , NVL(T1.LDFP_ACMLT_AMT         ,0) AS LDFP_ACMLT_AMT          /* LDFPAY적립금액           */
     , NVL(T1.LDFP_ACMLT_CNT         ,0) AS LDFP_ACMLT_CNT          /* LDFPAY적립건수           */
     , NVL(T1.LDFP_OURCOM_BDN_AMT    ,0) AS LDFP_OURCOM_BDN_AMT     /* LDFPAY당사부담금액       */
     , NVL(T1.LDFP_ALYCO_BDN_AMT     ,0) AS LDFP_ALYCO_BDN_AMT      /* LDFPAY제휴사부담금액     */
     , NVL(T1.FREELDFP_USE_AMT       ,0) AS FREELDFP_USE_AMT        /* 프리LDFPAY사용금액       */
     , NVL(T1.FREELDFP_USE_CNT       ,0) AS FREELDFP_USE_CNT        /* 프리LDFPAY사용건수       */
     , NVL(T1.FREELDFP_OURCOM_BDN_AMT,0) AS FREELDFP_OURCOM_BDN_AMT /* 프리LDFPAY당사부담금액   */
     , NVL(T1.FREELDFP_ALYCO_BDN_AMT ,0) AS FREELDFP_ALYCO_BDN_AMT  /* 프리LDFPAY제휴사부담금액 */
     , NVL(T1.DLR_CROS_ARSE_NSALAMT  ,0) AS DLR_CROS_ARSE_NSALAMT   /* 달러교차유발순매출액     */
     , NVL(T1.WON_CROS_ARSE_NSALAMT  ,0) AS WON_CROS_ARSE_NSALAMT   /* 원화교차유발순매출액     */
     , NVL(T1.OMNI_DLR_NSALAMT       ,0) AS OMNI_DLR_NSALAMT        /* 옴니달러순매출액         */
     , NVL(T1.OMNI_WON_NSALAMT       ,0) AS OMNI_WON_NSALAMT        /* 옴니원화순매출액         */
     , NVL(T1.OMNI_DLR_EXCH_NSALAMT  ,0) AS OMNI_DLR_EXCH_NSALAMT   /* 옴니달러교환권순매출액   */
     , NVL(T1.OMNI_WON_EXCH_NSALAMT  ,0) AS OMNI_WON_EXCH_NSALAMT   /* 옴니원화교환권순매출액   */
     , NVL(T1.FREELDFP_ACMLT_AMT     ,0) AS FREELDFP_ACMLT_AMT      /* 프리LDFPAY적립금액    */
     , NVL(T1.FREELDFP_ACMLT_CNT     ,0) AS FREELDFP_ACMLT_CNT      /* 프리LDFPAY적립건수    */
     , SYSDATE                           AS LOAD_DTTM               /* 적재일시                 */
     , 0                                 AS DLR_CROS_EXCH_NSALAMT   /*달러교차교환권순매출액   */
     , 0                                 AS WON_CROS_EXCH_NSALAMT   /*원화교차교환권순매출액   */
  FROM (
        SELECT /*+ PARALLEL(4) */
               T0.PRMTNCD                          AS PRMTNCD
             , T0.STR_CD                           AS STR_CD
             , T0.STD_DT                           AS STD_DT
             , MAX(T0.INTG_CUST_DISTING_NO)        AS INTG_CUST_DISTING_NO
             , T0.INTG_MEMB_NO                     AS INTG_MEMB_NO
             , MAX(T0.CUST_DISTING_NO       )      AS CUST_DISTING_NO
             , MAX(T0.CUST_SALES_DVS_CD     )      AS CUST_SALES_DVS_CD
             , MAX(T0.SL_CHNL_CD            )      AS SL_CHNL_CD
             , MAX(T0.CUST_CLSF_CD          )      AS CUST_CLSF_CD
             , MAX(T0.GRP_CLSF_CD           )      AS GRP_CLSF_CD
             , MAX(T0.IMGN_INFO_DVS_CD      )      AS IMGN_INFO_DVS_CD         /*출입국정보구분코드*/
             , MAX(T0.IMGN_DT               )      AS IMGN_DT                  /*출입국일자 */
             , SUM(T1.DLR_PDCST_SUM_AMT     )      AS DLR_PDCST_SUM_AMT        /* 달러원가합계금액         */
             , SUM(T1.WON_PDCST_SUM_AMT     )      AS WON_PDCST_SUM_AMT        /* 원화원가합계금액         */
             , SUM(T1.DLR_EXCH_NSALAMT      )      AS DLR_EXCH_NSALAMT         /* 달러교환권순매출액       */
             , SUM(T1.WON_EXCH_NSALAMT      )      AS WON_EXCH_NSALAMT         /* 원화교환권순매출액       */
             , SUM(T1.EXCH_CNT              )      AS EXCH_CNT                 /* 교환권건수               */
             , SUM(T1.DLR_PRMTN_NSALAMT     )      AS DLR_PRMTN_NSALAMT        /* 달러행사순매출액         */
             , SUM(T1.WON_PRMTN_NSALAMT     )      AS WON_PRMTN_NSALAMT        /* 원화행사순매출액         */
             , SUM(T1.PRMTN_EXCH_CNT        )      AS PRMTN_EXCH_CNT           /* 행사교환권건수           */
             , SUM(T1.DLR_ARSE_NSALAMT      )      AS DLR_ARSE_NSALAMT         /* 달러유발순매출액         */
             , SUM(T1.WON_ARSE_NSALAMT      )      AS WON_ARSE_NSALAMT         /* 원화유발순매출액         */
             , SUM(T2.DLR_PRMTN_DC_AMT      )      AS DLR_PRMTN_DC_AMT         /* 달러행사할인금액         */
             , SUM(T2.WON_PRMTN_DC_AMT      )      AS WON_PRMTN_DC_AMT         /* 원화행사할인금액         */
             , SUM(T2.DC_OURCOM_BDN_AMT     )      AS DC_OURCOM_BDN_AMT        /* 할인당사부담금액         */
             , SUM(T2.DC_ALYCO_BDN_AMT      )      AS DC_ALYCO_BDN_AMT         /* 할인제휴사부담금액       */
             , SUM(T2.DC_CNT                )      AS DC_CNT                   /* 할인건수                 */
             , SUM(T2.WON_TOT_DC_AMT        )      AS WON_TOT_DC_AMT           /* 원화총할인금액           */
             , SUM(T3.FGF_PRESTAT_AMT       )      AS FGF_PRESTAT_AMT          /* 사은품증정금액           */
             , SUM(T3.FGF_PRESTAT_CNT       )      AS FGF_PRESTAT_CNT          /* 사은품증정건수           */
             , SUM(T3.PRESTAT_OURCOM_BDN_AMT)      AS PRESTAT_OURCOM_BDN_AMT   /* 증정당사부담금액         */
             , SUM(T3.PRESTAT_ALYCO_BDN_AMT )      AS PRESTAT_ALYCO_BDN_AMT    /* 증정제휴사부담금액       */
             , SUM(T3.TOT_PRESTAT_AMT       )      AS TOT_PRESTAT_AMT          /* 총증정금액               */
             , SUM(T3.PRMTN_PRESTAT_AMT     )      AS PRMTN_PRESTAT_AMT        /* 행사증정금액             */
             , SUM(T3.PRMTN_CASH_RTRV_AMT   )      AS PRMTN_CASH_RTRV_AMT      /* 행사현금회수금액         */
             , SUM(T4.LDFP_ACMLT_AMT        )      AS LDFP_ACMLT_AMT           /* LDFPAY적립금액           */
             , SUM(T4.LDFP_ACMLT_CNT        )      AS LDFP_ACMLT_CNT           /* LDFPAY적립건수           */
             , SUM(T4.LDFP_OURCOM_BDN_AMT   )      AS LDFP_OURCOM_BDN_AMT      /* LDFPAY당사부담금액       */
             , SUM(T4.LDFP_ALYCO_BDN_AMT    )      AS LDFP_ALYCO_BDN_AMT       /* LDFPAY제휴사부담금액     */
             , SUM(T5.FREELDFP_USE_AMT       )     AS FREELDFP_USE_AMT         /* 프리LDFPAY사용금액       */
             , SUM(T5.FREELDFP_USE_CNT       )     AS FREELDFP_USE_CNT         /* 프리LDFPAY사용건수       */
             , SUM(T5.FREELDFP_OURCOM_BDN_AMT)     AS FREELDFP_OURCOM_BDN_AMT  /* 프리LDFPAY당사부담금액   */
             , SUM(T5.FREELDFP_ALYCO_BDN_AMT )     AS FREELDFP_ALYCO_BDN_AMT   /* 프리LDFPAY제휴사부담금액 */
             , SUM(T1.DLR_CROS_ARSE_NSALAMT  )     AS DLR_CROS_ARSE_NSALAMT    /* 달러교차유발순매출액     */
             , SUM(T1.WON_CROS_ARSE_NSALAMT  )     AS WON_CROS_ARSE_NSALAMT    /* 원화교차유발순매출액     */
             /* 20220803 옴니채널 추가 */
             , SUM(T1.OMNI_DLR_NSALAMT      )      AS OMNI_DLR_NSALAMT         /* 옴니달러순매출액         */
             , SUM(T1.OMNI_WON_NSALAMT      )      AS OMNI_WON_NSALAMT         /* 옴니원화순매출액         */
             , SUM(T1.OMNI_DLR_EXCH_NSALAMT )      AS OMNI_DLR_EXCH_NSALAMT    /* 옴니달러교환권순매출액   */
             , SUM(T1.OMNI_WON_EXCH_NSALAMT )      AS OMNI_WON_EXCH_NSALAMT    /* 옴니원화교환권순매출액   */
             , SUM(T4.FREELDFP_ACMLT_AMT    )      AS FREELDFP_ACMLT_AMT       /* 프리LDFPAY적립금액 */
             , SUM(T4.FREELDFP_ACMLT_CNT    )      AS FREELDFP_ACMLT_CNT       /* 프리LDFPAY적립건수 */
          FROM (
                SELECT /*+ USE_HASH(T0 T10) PARALLEL(4) */
                       T0.PRMTNCD
                     , T0.STR_CD
                     , T0.STD_DT
                     , T0.INTG_MEMB_NO
                     , MAX(T1.INTG_CUST_DISTING_NO) AS INTG_CUST_DISTING_NO
                     , MAX(T1.CUST_DISTING_NO     ) AS CUST_DISTING_NO
                     , MIN(T1.CUST_SALES_DVS_CD   ) AS CUST_SALES_DVS_CD
                     , MAX(T1.SL_CHNL_CD          ) AS SL_CHNL_CD
                     , MAX(T1.CUST_CLSF_CD        ) AS CUST_CLSF_CD
                     , MAX(T1.GRP_CLSF_CD         ) AS GRP_CLSF_CD
                     , MAX(T1.IMGN_INFO_DVS_CD    ) AS IMGN_INFO_DVS_CD  /*출입국정보구분코드*/
                     , MAX(T1.IMGN_DT             ) AS IMGN_DT           /*출입국일자 */
                  FROM (
                       SELECT /*+ PARALLEL(2) */
                              T0.STR_CD 
                            , T0.STD_DT
                            , T0.PRMTNCD
                            , T0.INTG_MEMB_NO
                         FROM FL_MK_PRMTN_EXCH_SL T0  /* FL_MK_행사교환권판매 */
                        INNER JOIN TEMP_OFLN_PRMTN_CHNG T1  /* 변경적재 대상 */
                           ON T0.PRMTNCD           = T1.PRMTNCD
                          AND T0.STR_CD            = T1.STR_CD
                          AND T0.INTG_MEMB_NO      = T1.INTG_MEMB_NO 
                        GROUP BY T0.STR_CD 
                            , T0.STD_DT
                            , T0.PRMTNCD
                            , T0.INTG_MEMB_NO
                        UNION  /* 구매증정 데이터 중 매출일과 증정일자가 상이하여 행사교환권판매에서 빠지는 적립건 추가 */
                       SELECT /*+ PARALLEL(2) */
                              T1.STR_CD 
                            , T1.STD_DT
                            , T1.PRMTNCD
                            , T1.INTG_MEMB_NO
                         FROM FL_MK_PRMTN_EXCH_CHNG_TEMP T1
                        INNER JOIN D_PRMTN T2
                           ON T1.PRMTNCD = T2.PRMTNCD 
                          AND T2.PRMTN_LGCSF_CD IN ('003','005')
                        WHERE T1.INTG_MEMB_NO IS NOT NULL
                        GROUP BY T1.STR_CD 
                            , T1.STD_DT
                            , T1.PRMTNCD
                            , T1.INTG_MEMB_NO
                       ) T0  /* FL_MK_행사교환권판매 */
                  LEFT OUTER JOIN  FL_MK_PRMTN_EXCH_SL T1  /* 변경적재 대상 */
                    ON T0.PRMTNCD           = T1.PRMTNCD
                   AND T0.STR_CD            = T1.STR_CD
                   AND T0.INTG_MEMB_NO      = T1.INTG_MEMB_NO
                   AND T0.STD_DT            = T1.STD_DT
	   	         GROUP BY T0.PRMTNCD
                     , T0.STR_CD
                     , T0.STD_DT
                     , T0.INTG_MEMB_NO              
             ) T0
              /* 교환권 관련 매출 */
          LEFT OUTER JOIN TEMP_PRMTN_ACTRSLT_01 T1
            ON T0.PRMTNCD           = T1.PRMTNCD
           AND T0.STR_CD            = T1.STR_CD
           AND T0.STD_DT            = T1.STD_DT
           AND T0.INTG_MEMB_NO      = T1.INTG_MEMB_NO
               /* 할인금액 */
          LEFT OUTER JOIN TEMP_PRMTN_ACTRSLT_02 T2
            ON T0.PRMTNCD           = T2.PRMTNCD
           AND T0.STR_CD            = T2.STR_CD
           AND T0.STD_DT            = T2.STD_DT
           AND T0.INTG_MEMB_NO      = T2.INTG_MEMB_NO
               /* 사은품증정금액 */
          LEFT OUTER JOIN TEMP_PRMTN_ACTRSLT_03  T3
            ON T0.PRMTNCD           = T3.PRMTNCD
           AND T0.STR_CD            = T3.STR_CD
           AND T0.STD_DT            = T3.STD_DT
           AND T0.INTG_MEMB_NO      = T3.INTG_MEMB_NO
               /* LDFPAY증정금액-프리LDFPAY제외 */
          LEFT OUTER JOIN TEMP_PRMTN_ACTRSLT_04 T4
            ON T0.PRMTNCD           = T4.PRMTNCD
           AND T0.STR_CD            = T4.STR_CD
           AND T0.STD_DT            = T4.STD_DT
           AND T0.INTG_MEMB_NO      = T4.INTG_MEMB_NO
               /* 프리LDFPAY 사용금액 */
         LEFT OUTER JOIN TEMP_PRMTN_ACTRSLT_05 T5
            ON T0.PRMTNCD           = T5.PRMTNCD
           AND T0.STR_CD            = T5.STR_CD
           AND T0.STD_DT            = T5.STD_DT
           AND T0.INTG_MEMB_NO      = T5.INTG_MEMB_NO
         GROUP BY T0.PRMTNCD
                , T0.STR_CD
                , T0.STD_DT
                , T0.INTG_MEMB_NO
     ) T1
  LEFT OUTER JOIN D_MEMB T2 /* D_회원 */
    ON T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
  LEFT OUTER JOIN WL_SL_STR_INFO T3
    ON T1.STR_CD = T3.STR_CD
 INNER JOIN D_INTG_MEMB T6
    ON T1.INTG_MEMB_NO = T6.INTG_MEMB_NO
  LEFT OUTER JOIN WL_SL_PSPT_RCGNT_NO T4
    ON NVL(T6.PSPT_RCGNT_NO, T2.PSPT_RCGNT_NO) = T4.PSPT_RCGNT_NO   /* 여권인식번호 */
 INNER JOIN (
       SELECT T1.PRMTNCD 
            , T1.PRMTN_LGCSF_CD
            , T1.PRMTN_MDCSF_CD 
            , T1.MDPRCD 
            , T1.LGPRCD
            , T1.CMPN_OFFER_NO
         FROM D_PRMTN t1
        WHERE NOT EXISTS (SELECT 1 FROM D_LGPROMO T2 WHERE T1.LGPRCD = T2.LGPRCD AND T2.PRMTN_ACTRSLT_OBJ_XCLUD_YN = 'Y')  /* 행사실적대상제외여부 */
       ) T5
    ON T1.PRMTNCD = T5.PRMTNCD
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_MERGE_01  *****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

MERGE /*+ PARALLEL(4) */
 INTO FL_MK_PRMTN_ACTRSLT T1
USING (SELECT T2.PRMTNCD
            , T2.STR_CD
            , T2.STD_DT
            , T2.INTG_MEMB_NO
            , T2.DLR_EXCH_NSALAMT
            , T2.WON_EXCH_NSALAMT
         FROM FL_MK_PRMTN_ACTRSLT T2
        INNER JOIN
            ( SELECT DISTINCT
                     T2.PRMTNCD
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
                  ON T1.CAMP_ID       = T3.PRMTNCD
                 AND T1.VIP_NO        = T3.CHNG_VIP_NO
               WHERE NVL(TRIM(T1.SEND_MEMB_CD),T2.CHNL_TYPE_CD ) <> T2.CHNL_TYPE_CD
            ) T3
           ON T2.PRMTNCD           = T3.PRMTNCD
          AND T2.CUST_DISTING_NO   = T3.VIP_NO
          AND T2.STR_CD            = T3.STR_CD
      ) T2
   ON (T1.PRMTNCD      = T2.PRMTNCD
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