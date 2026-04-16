

SELECT * FROM 실수로 실행시 에러발생 필요 

ALTER SESSION ENABLE PARALLEL DML;


DROP TABLE TEMP_PRMTN_OFFER_ACTRSLT_01_INIT

CREATE TABLE TEMP_PRMTN_OFFER_ACTRSLT_01_INIT NOLOGGING AS
SELECT 
       T1.PRMTNCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.INTG_MEMB_NO
     , T1.CMPN_OFFER_NO  
     , T1.PRMTN_SECTRG_NO  
     , T1.CUST_SALES_DVS_CD
     , T1.DLR_PDCST_SUM_AMT
     , T1.WON_PDCST_SUM_AMT
     , T1.DLR_EXCH_NSALAMT
     , T1.WON_EXCH_NSALAMT
     , T1.EXCH_CNT
     , T4.DLR_PRMTN_NSALAMT
     , T4.WON_PRMTN_NSALAMT
     , T1.PRMTN_EXCH_CNT
     , T4.OMNI_DLR_NSALAMT    /* 옴니달러순매출액 */
     , T4.OMNI_WON_NSALAMT    /* 옴니원화순매출액 */
     , T1.OMNI_DLR_EXCH_NSALAMT  /* 옴니달러교환권순매출액 */
     , T1.OMNI_WON_EXCH_NSALAMT  /* 옴니원화교환권순매출액 */
     , T1.DLR_ARSE_NSALAMT        /* 달러유발순매출액        */
     , T1.WON_ARSE_NSALAMT        /* 원화유발순매출액        */
     , T1.DLR_CROS_ARSE_NSALAMT   /* 달러교차유발순매출액     */
     , T1.WON_CROS_ARSE_NSALAMT   /* 원화교차유발순매출액     */
 FROM (
SELECT /*+ PARALLEL(4) */
       T1.PRMTNCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.INTG_MEMB_NO
     , NVL(T1.CMPN_OFFER_NO   , T4.CMPN_OFFER_NO  )  CMPN_OFFER_NO
     , NVL(T1.PRMTN_SECTRG_NO , T4.PRMTN_SECTRG_NO)  PRMTN_SECTRG_NO
     , MIN(T1.CUST_SALES_DVS_CD)                      AS CUST_SALES_DVS_CD
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN NVL(T3.DLR_PDCST,0)*T1.SALES_SIGN ELSE 0 END)         AS DLR_PDCST_SUM_AMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN NVL(T3.WON_PDCST,0)*T1.SALES_SIGN ELSE 0 END)         AS WON_PDCST_SUM_AMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS DLR_EXCH_NSALAMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS WON_EXCH_NSALAMT
     , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.SALES_SIGN ELSE 0 END) AS EXCH_CNT
     , SUM(T1.SALES_SIGN) AS PRMTN_EXCH_CNT
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' AND EXCH_PRMTN_APLY_YN ='Y' THEN ROUND(T1.DLR_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_DLR_EXCH_NSALAMT  /* 옴니달러교환권순매출액 */
     , SUM(CASE WHEN T1.ONLN_SL_DVS_CD = '5' AND EXCH_PRMTN_APLY_YN ='Y' THEN ROUND(T1.WON_TOT_NSALAMT*T1.SALES_SIGN,0) ELSE 0 END) AS OMNI_WON_EXCH_NSALAMT  /* 옴니원화교환권순매출액 */
     , SUM(T1.DLR_ARSE_NSALAMT*T1.SALES_SIGN) AS DLR_ARSE_NSALAMT        /* 달러유발순매출액        */
     , SUM(T1.WON_ARSE_NSALAMT*T1.SALES_SIGN) AS WON_ARSE_NSALAMT        /* 원화유발순매출액        */
     , SUM(CASE WHEN T1.SL_CHNL_CD IN ('4','5') THEN T1.DLR_ARSE_NSALAMT*T1.SALES_SIGN ELSE 0 END) AS DLR_CROS_ARSE_NSALAMT   /* 달러교차유발순매출액     */
     , SUM(CASE WHEN T1.SL_CHNL_CD IN ('4','5') THEN T1.WON_ARSE_NSALAMT*T1.SALES_SIGN ELSE 0 END) AS WON_CROS_ARSE_NSALAMT   /* 원화교차유발순매출액     */ 
  FROM FL_MK_PRMTN_EXCH_SL T1         /* FL_MK_행사교환권판매 */
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
                WHERE T1.STD_DT >= '20190101'  --AND   T1.PRMTNCD = '2311626001'
             )   T2 
           ON T1.SL_DT   = T2.STD_DT
          AND T1.STR_CD  = T2.STR_CD
          AND T1.EXCH_NO = T2.EXCH_NO
        WHERE T1.SL_DT >= '20190101'
        GROUP BY T1.STR_CD
               , T1.SL_DT
               , T1.EXCH_NO
     )  T3
    ON T1.STR_CD  = T3.STR_CD
   AND T1.STD_DT  = T3.SL_DT
   AND T1.EXCH_NO = T3.EXCH_NO
 LEFT OUTER JOIN 
    (  SELECT DISTINCT 
              PRMTNCD
            , STR_CD
            , STD_DT
            , INTG_MEMB_NO
            , CMPN_OFFER_NO
            , PRMTN_SECTRG_NO    
        FROM (
        SELECT /*+ PARALLEL(4) */
              PRMTNCD
            , T1.STR_CD
            , T1.STD_DT
            , T1.INTG_MEMB_NO
            , T1.EXCH_NO
            , MAX(T1.CMPN_OFFER_NO  ) AS CMPN_OFFER_NO
            , MAX(T1.PRMTN_SECTRG_NO) AS PRMTN_SECTRG_NO
       FROM FL_MK_PRMTN_EXCH_SL T1
       WHERE EXCH_PRMTN_APLY_YN = 'Y' --AND   T1.PRMTNCD = 'S231071004'
       GROUP BY PRMTNCD
            , T1.STR_CD
            , T1.STD_DT
            , T1.INTG_MEMB_NO
            , T1.EXCH_NO
         ) T1
    ) T4
    ON T1.PRMTNCD = T4.PRMTNCD
   AND T1.STR_CD  = T4.STR_CD
   AND T1.STD_DT  = T4.STD_DT
   AND T1.INTG_MEMB_NO = T4.INTG_MEMB_NO
   AND T1.EXCH_PRMTN_APLY_YN = 'N'
 --WHERE  T1.PRMTNCD = 'S231071004'
-- where T1.STD_DT >= '20190101'  AND PRMTNCD = 'S1912NP003'
 GROUP BY T1.PRMTNCD
        , T1.STR_CD
        , T1.STD_DT
        , T1.INTG_MEMB_NO
        , NVL(T1.CMPN_OFFER_NO   , T4.CMPN_OFFER_NO  )  
        , NVL(T1.PRMTN_SECTRG_NO , T4.PRMTN_SECTRG_NO) 
 ) T1        
 LEFT OUTER JOIN FL_MK_PRMTN_ACTRSLT T4
              ON T1.PRMTNCD      = T4.PRMTNCD
             AND T1.STR_CD       = T4.STR_CD
             AND T1.STD_DT       = T4.STD_DT
             AND T1.INTG_MEMB_NO = T4.INTG_MEMB_NO
             
        
  --9227300      
--------------------------------------------------------------------------------------------------             
        
DROP TABLE TEMP_PRMTN_OFFER_ACTRSLT_02_INIT        
        
CREATE TABLE TEMP_PRMTN_OFFER_ACTRSLT_02_INIT NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       T1.PRMTNCD
     , T1.STR_CD
     , T1.STD_DT
     , T1.INTG_MEMB_NO
     , NVL(T1.CMPN_OFFER_NO,0) CMPN_OFFER_NO
     , NVL(T1.PRMTN_SECTRG_NO,0)    PRMTN_SECTRG_NO
     , MIN(T1.CUST_SALES_DVS_CD)                   AS CUST_SALES_DVS_CD  /* 1:회원매출, 2:비회원매출, 3:미인식매출 */
     , SUM(CASE WHEN T2.OURCOM_BDNRT IS NOT NULL THEN (T5.WON_DC_AMT*T2.OURCOM_BDNRT/100.0)*T1.SALES_SIGN ELSE (T5.WON_DC_AMT*NVL(T3.OURCOM_BDNRT,100)/100.0)*T1.SALES_SIGN END)      AS DC_OURCOM_BDN_AMT
     , SUM(CASE WHEN T2.OURCOM_BDNRT IS NOT NULL THEN (T5.WON_DC_AMT*(100.0-T2.OURCOM_BDNRT)/100.0)*T1.SALES_SIGN ELSE (T5.WON_DC_AMT*NVL(T3.ALYCO_BDNRT,0)/100.0)*T1.SALES_SIGN END) AS DC_ALYCO_BDN_AMT
     , SUM(ROUND(T5.DLR_DC_AMT*T1.SALES_SIGN,0))  AS DLR_PRMTN_DC_AMT
     , SUM(ROUND(T5.WON_DC_AMT*T1.SALES_SIGN,0))  AS WON_PRMTN_DC_AMT
     , SUM(T1.SALES_SIGN)                          AS DC_CNT          /* 할인건수  */
     , SUM(T4.WON_TOT_DC_AMT)                      AS WON_TOT_DC_AMT
  FROM FL_MK_PRMTN_EXCH_SL T1        /* FL_MK_행사교환권판매 */
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
         INNER JOIN D_PRMTN T2 /* D_행사 */
            ON T1.PRMTNCD      = T2.PRMTNCD
          LEFT OUTER JOIN WL_LC_PRMTN_OFFER_SECT T3 /* WL_LC_행사OFFER구간 */
            ON T1.CMPN_OFFER_NO   = T3.CMPN_OFFER_NO
           AND T1.PRMTN_SECTRG_NO = T3.PRMTN_SECTRG_NO
         WHERE T1.PRMTN_LGCSF_CD  = '001'  /* 할인 */
           AND T1.STD_DT >= '20190101'
         GROUP BY T1.STD_DT
                , T1.STR_CD
                , T1.INTG_MEMB_NO
                , T1.CUST_SALES_DVS_CD
        ) T4
    ON T1.STR_CD            = T4.STR_CD
   AND T1.STD_DT            = T4.STD_DT
   AND T1.INTG_MEMB_NO      = T4.INTG_MEMB_NO
   AND T1.CUST_SALES_DVS_CD = T4.CUST_SALES_DVS_CD
  WHERE  T1.STD_DT >= '20190101' 
 GROUP BY T1.PRMTNCD
        , T1.STR_CD
        , T1.STD_DT
        , T1.INTG_MEMB_NO
     , NVL(T1.CMPN_OFFER_NO,0) 
     , NVL(T1.PRMTN_SECTRG_NO,0)          
        
 --1377133     
        
 -------------------------------------------------------------------------------------------------------------------       
    DROP TABLE TEMP_PRMTN_OFFER_ACTRSLT_03_INIT
    
     CREATE TABLE TEMP_PRMTN_OFFER_ACTRSLT_03_INIT NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       A01.PRMTNCD
     , A01.STR_CD
     , A01.STD_DT
     , A01.INTG_MEMB_NO
     , A01.CMPN_OFFER_NO
     , A01.PRMTN_SECTRG_NO 
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
              T2.PRMTNCD
            , T2.PYMT_STD_DT STD_DT
            , T2.STR_CD
            , T2.INTG_MEMB_NO
            , NVL(T2.CMPN_OFFER_NO,0) CMPN_OFFER_NO
            , NVL(T2.PRMTN_SECTRG_NO,0)  PRMTN_SECTRG_NO
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
         FROM FL_MK_PRMTN_FGF_PRESTAT_DTL T2  /* FL_MK_행사사은품증정상세 */
        INNER JOIN D_PRMTN T4  /* D_행사 */
           ON T2.PRMTNCD = T4.PRMTNCD
         LEFT OUTER JOIN WL_MK_FGF_BASIC_INFO T3 /* WL_MK_사은품기본정보 */
           ON T2.FGFCD = T3.FGFCD
         LEFT OUTER JOIN WL_MK_PRMTN_FGF_PRESTAT_DTL T10  /* WL_MK_행사사은품증정상세 */
           ON T2.ORIG_PRESTAT_SN = T10.PRESTAT_SN
          AND T10.FGF_RTRV_METHD_CD = '5'    /* 기타회수 당사부담금 0원 (기타회수의 경우 사은품금액이 제대로 안들어옴) */
        WHERE T4.PRMTN_LGCSF_CD     = '003'  /* 사은품 */
          AND T2.PYMT_STD_DT >= '20190101'
         -- AND T2.PRMTNCD = '2311309007' AND T2.PYMT_STD_DT BETWEEN '20231206' AND '20231206' AND T2.INTG_MEMB_NO = '2029348013'
        GROUP BY T2.PRMTNCD
               , T2.PYMT_STD_DT 
               , T2.STR_CD
               , T2.INTG_MEMB_NO
               , NVL(T2.CMPN_OFFER_NO,0) 
               , NVL(T2.PRMTN_SECTRG_NO,0)  
       HAVING SUM(CASE WHEN T2.PRESTAT_DVS_CD = '0' THEN 1 ELSE -1 END)  <> 0
      ) A01
  INNER JOIN (
       SELECT /*+ USE_HASH(T1 T2) PARALLEL(4)*/
              T2.PYMT_STD_DT STD_DT
            , T2.STR_CD
            , T2.INTG_MEMB_NO
            , NVL(T2.CMPN_OFFER_NO,0) CMPN_OFFER_NO
            , NVL(T2.PRMTN_SECTRG_NO,0)  PRMTN_SECTRG_NO
            , MIN(T2.CUST_SALES_DVS_CD) AS CUST_SALES_DVS_CD
            , SUM( DECODE(T2.PRESTAT_DVS_CD,'0',1,-1) * (
                        CASE WHEN NVL(T2.CASH_RTRV_AMT,0) = 0 THEN (
                             CASE WHEN ( T2.PRESTAT_DVS_CD = '1' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T2.FGF_RTRV_METHD_CD ='5' ) THEN 0   /* 기타회수 */
                                  WHEN ( T2.PRESTAT_DVS_CD = '0' AND NVL(T2.PRESTAT_AMT,0) = 0 AND T10.PRESTAT_SN IS NOT NULL ) THEN 0  /* 증정의 원증정일련번호의 사은품회수방법코드가 '5' 기타회수면 당사부당금은 0 */
                                 ELSE NVL(T3.FGF_UPRC,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0) END )
                             WHEN NVL(T2.CASH_RTRV_AMT,0) <> 0 THEN (NVL(T2.CASH_RTRV_AMT,0) - NVL(ABS(T2.ALYCO_BDN_AMT),0))
                         END ) )  AS TOT_PRESTAT_AMT
         FROM FL_MK_PRMTN_FGF_PRESTAT_DTL T2  /* FL_MK_행사사은품증정상세 */
        INNER JOIN D_PRMTN T4  /* D_행사 */
           ON T2.PRMTNCD = T4.PRMTNCD
         LEFT OUTER JOIN WL_MK_FGF_BASIC_INFO T3 /* WL_MK_사은품기본정보 */
           ON T2.FGFCD   = T3.FGFCD
         LEFT OUTER JOIN WL_MK_PRMTN_FGF_PRESTAT_DTL T10  /* WL_MK_행사사은품증정상세 */
           ON T2.ORIG_PRESTAT_SN = T10.PRESTAT_SN
          AND T10.FGF_RTRV_METHD_CD = '5'    /* 기타회수 당사부담금 0원 (기타회수의 경우 사은품금액이 제대로 안들어옴) */
        WHERE T4.PRMTN_LGCSF_CD     = '003'  /* 사은품 */
         AND T2.PYMT_STD_DT >= '20190101'
          -- AND T2.PRMTNCD = '2311309007' AND T2.PYMT_STD_DT BETWEEN '20231206' AND '20231206' AND T2.INTG_MEMB_NO = '2029348013'
       GROUP BY T2.PYMT_STD_DT 
              , T2.STR_CD
              , T2.INTG_MEMB_NO
              , NVL(T2.CMPN_OFFER_NO,0) 
              , NVL(T2.PRMTN_SECTRG_NO,0)  
        HAVING SUM(CASE WHEN T2.PRESTAT_DVS_CD = '0' THEN 1 ELSE -1 END)  <> 0
      ) A02
    ON A01.STD_DT       = A02.STD_DT
   AND A01.STR_CD       = A02.STR_CD
   AND A01.INTG_MEMB_NO = A02.INTG_MEMB_NO
   AND A01.CMPN_OFFER_NO = A02.CMPN_OFFER_NO
   AND A01.PRMTN_SECTRG_NO = A02.PRMTN_SECTRG_NO 
;

----------------------------------------------------------------------------------------------------------------------------------


DROP TABLE TEMP_PRMTN_OFFER_ACTRSLT_04_INIT

CREATE TABLE TEMP_PRMTN_OFFER_ACTRSLT_04_INIT  NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       K1.PRMTNCD
     , K1.PYMT_STD_DT AS STD_DT
     , K1.STR_CD
     , K1.INTG_MEMB_NO
     , K1.CMPN_OFFER_NO
     , K1.PRMTN_SECTRG_NO 
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
  FROM  WL_MK_PRMTN_FGF_PRESTAT K1
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
    ON K2.CMPN_OFFER_NO   = K5.CMPN_OFFER_NO
   AND K2.PRMTN_SECTRG_NO = K5.PRMTN_SECTRG_NO
   AND K2.SEQ             = K5.SEQ
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
 WHERE K1.PRESTAT_DT >= '20190101'  
 GROUP BY K1.PRMTNCD
        , K1.PYMT_STD_DT
        , K1.STR_CD
        , K1.INTG_MEMB_NO
        , K1.CMPN_OFFER_NO
        , K1.PRMTN_SECTRG_NO 
HAVING SUM(CASE WHEN K1.PRESTAT_DVS_CD = '0' THEN 1 ELSE -1 END)  <> 0


---------------------------------------------------------------------------------------------------------------

DROP TABLE TEMP_PRMTN_OFFER_ACTRSLT_05_INIT

CREATE TABLE TEMP_PRMTN_OFFER_ACTRSLT_05_INIT NOLOGGING AS
/* 프리 LDFPAY 사용 */
SELECT /*+ USE_HASH(T1 T3 T7 T4) PARALLEL(4) */
       T1.PRMTNCD
     , T1.STD_DT
     , T1.STR_CD
     , T1.INTG_MEMB_NO
     , T3.CMPN_OFFER_NO
     , T3.PRMTN_SECTRG_NO   
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
          FROM FL_MK_PRMTN_EXCH_SL  T1         /* FL_MK_행사교환권판매 */
          WHERE T1.STD_DT >= '20190101'
         GROUP BY T1.PRMTNCD
                , T1.STD_DT
                , T1.STR_CD
                , T1.INTG_MEMB_NO
     ) T1
 INNER JOIN FL_MK_LDFP_ACMLT_USE_PTCLS T3  /* 해당테이블에 ACMLT_USE_DVS_CD= '2'(사용)인 경우 LRWD_NO별로 사용,사용취소가 집계되어 있음 */
    ON T1.PRMTNCD      = T3.PRMTNCD
   AND T1.STR_CD       = T3.STR_CD 
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
  WHERE T3.LDFP_HIST_HAPN_DT >= '20190101'
 GROUP BY T1.PRMTNCD
        , T1.STD_DT
        , T1.STR_CD
        , T1.INTG_MEMB_NO
        , T3.CMPN_OFFER_NO
        , T3.PRMTN_SECTRG_NO   
 HAVING SUM(CASE WHEN T3.LDFP_RCPDSBS_TYPE_CD in('21','26') THEN T3.LDFP_USE_AMT ELSE 0 END) <> 0
 
-------------------------------------------------------------------------------------------------------------------------- 
 DROP TABLE FL_MK_PRMTN_EXCH_CHNG_TEMP_INIT
 
 CREATE TABLE FL_MK_PRMTN_EXCH_CHNG_TEMP_INIT AS 
SELECT /*+ PARALLEL(4) */
       STR_CD
     , STD_DT
     , MAX(EXCH_NO)    AS EXCH_NO
     , PRMTNCD
     , INTG_MEMB_NO
     , SYSDATE         AS LOAD_DTTM
     , MAX(VIP_NO)     AS CHNG_VIP_NO
     , CMPN_OFFER_NO
     , PRMTN_SECTRG_NO
  FROM (
       SELECT STR_CD
            , PRESTAT_DT  AS STD_DT
            , CAST(NULL  AS VARCHAR(20))        AS EXCH_NO
            , PRMTNCD
            , INTG_MEMB_NO
            , VIP_NO
            , CMPN_OFFER_NO
            , PRMTN_SECTRG_NO
         FROM WL_MK_PRMTN_FGF_PRESTAT /* WL_MK_행사사은품증정 */
        WHERE (PRESTAT_DT  >='20190101' 
           OR  PYMT_STD_DT >= '20190101' 
          )
       ) T
 GROUP BY STR_CD
        , STD_DT
        , PRMTNCD
        , INTG_MEMB_NO
        , CMPN_OFFER_NO
        , PRMTN_SECTRG_NO                          
        
        
-------------------------------------------------------------------------------------------------------------------

  DROP TABLE FL_MK_PRMTN_OFFER_ACTRSLT_INIT
  
CREATE TABLE FL_MK_PRMTN_OFFER_ACTRSLT_INIT AS   
SELECT /*+ PARALLEL(4) */
       T1.PRMTNCD                 /* 행사코드             */
     , T1.STR_CD                  /* 점코드               */
     , T1.STD_DT                  /* 기준일자             */
     , T1.INTG_MEMB_NO            /* 통합회원번호           */
     , T1.CMPN_OFFER_NO           OFFER_NO
     , T1.PRMTN_SECTRG_NO
     , T1.INTG_CUST_DISTING_NO
     , CASE WHEN T1.INTG_MEMB_NO IS NOT NULL THEN 'Y' ELSE 'N' END AS INTG_MEMB_YN
     , T1.CUST_DISTING_NO         /* 고객식별번호           */
	 , NVL(T6.PSPT_RCGNT_NO, T2.PSPT_RCGNT_NO)  AS PSPT_RCGNT_NO             /* 여권인식번호           */
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
     , MAX(NVL(T1.WON_TOT_DC_AMT,0)) OVER (PARTITION BY T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO, T1.CUST_DISTING_NO) AS WON_TOT_DC_AMT          /* 원화총할인금액  , T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO       */
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
     , 0 DLR_CROS_EXCH_NSALAMT
     , 0 WON_CROS_EXCH_NSALAMT
     , SYSDATE                           AS LOAD_DTTM               /* 적재일시                 */
--             SELECT  SUM(NVL(T1.WON_EXCH_NSALAMT       ,0))
--     SELECT *
  FROM (
        SELECT /*+ PARALLEL(4) */
               T0.PRMTNCD                          AS PRMTNCD
             , T0.STR_CD                           AS STR_CD
             , T0.STD_DT                           AS STD_DT
             , T0.INTG_CUST_DISTING_NO             AS INTG_CUST_DISTING_NO
             , T0.INTG_MEMB_NO                     AS INTG_MEMB_NO
             , NVL(COALESCE(T1.CMPN_OFFER_NO,T2.CMPN_OFFER_NO,T3.CMPN_OFFER_NO
                           ,T4.CMPN_OFFER_NO,T5.CMPN_OFFER_NO),0)   AS CMPN_OFFER_NO
             , NVL(COALESCE(T1.PRMTN_SECTRG_NO,T2.PRMTN_SECTRG_NO,T3.PRMTN_SECTRG_NO
                       ,T4.PRMTN_SECTRG_NO,T5.PRMTN_SECTRG_NO),0)      AS PRMTN_SECTRG_NO
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
             , SUM(T1.EXCH_CNT              )      AS EXCH_CNT                 /* 교환권건수                 */
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
      -- SELECT SUM(T1.DLR_ARSE_NSALAMT      )
       FROM (
                SELECT /*+ USE_HASH(T0 T10) PARALLEL(4) */
                       T0.PRMTNCD
                     , T0.STR_CD
                     , T0.STD_DT
                     , T0.INTG_MEMB_NO
                     , T0.CMPN_OFFER_NO 
                     , T0.PRMTN_SECTRG_NO 
                     , MAX(T1.INTG_CUST_DISTING_NO) AS INTG_CUST_DISTING_NO
                     , MAX(T1.CUST_DISTING_NO     ) AS CUST_DISTING_NO
                     , MIN(T1.CUST_SALES_DVS_CD   ) AS CUST_SALES_DVS_CD
                     , MAX(T1.SL_CHNL_CD          ) AS SL_CHNL_CD
                     , MAX(T1.CUST_CLSF_CD        ) AS CUST_CLSF_CD
                     , MAX(T1.GRP_CLSF_CD         ) AS GRP_CLSF_CD
                     , MAX(T1.IMGN_INFO_DVS_CD    ) AS IMGN_INFO_DVS_CD  /*출입국정보구분코드*/
                     , MAX(T1.IMGN_DT             ) AS IMGN_DT           /*출입국일자 */
                  FROM (
                        SELECT  /*+ PARALLEL(8) */ 
                                T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO ,NVL(T1.CMPN_OFFER_NO,0) CMPN_OFFER_NO, NVL(T1.PRMTN_SECTRG_NO,0) PRMTN_SECTRG_NO
                        FROM (  
                                SELECT  T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO ,T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO      FROM TEMP_PRMTN_OFFER_ACTRSLT_01_INIT T1
                                UNION ALL
                                SELECT  T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO ,T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO      FROM TEMP_PRMTN_OFFER_ACTRSLT_02_INIT T1
                                UNION ALL
                                SELECT  T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO ,T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO      FROM TEMP_PRMTN_OFFER_ACTRSLT_03_INIT T1
                                UNION ALL
                                SELECT  T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO ,T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO      FROM TEMP_PRMTN_OFFER_ACTRSLT_04_INIT T1
                                UNION ALL
                                SELECT  T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO ,T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO      FROM TEMP_PRMTN_OFFER_ACTRSLT_05_INIT T1
                                UNION ALL 
                                SELECT  T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO ,T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO      FROM FL_MK_PRMTN_EXCH_CHNG_TEMP_INIT  T1  WHERE INTG_MEMB_NO IS NOT NULL
                               ) T1  -- WHERE PRMTNCD = 'S231071004'
                        GROUP BY  T1.PRMTNCD, T1.STR_CD,T1.STD_DT,T1.INTG_MEMB_NO,NVL(T1.CMPN_OFFER_NO,0) , NVL(T1.PRMTN_SECTRG_NO,0) 
                        ) T0  /* FL_MK_행사교환권판매 */
                  LEFT OUTER JOIN  FL_MK_PRMTN_EXCH_SL T1  /* 변경적재 대상 */
                    ON T0.PRMTNCD           = T1.PRMTNCD
                   AND T0.STR_CD            = T1.STR_CD
                   AND T0.INTG_MEMB_NO      = T1.INTG_MEMB_NO
                   AND T0.STD_DT            = T1.STD_DT
                -- WHERE 1=1 AND  T0.PRMTNCD = '2311626001'
                 --AND T0.STD_DT BETWEEN '20240129' AND '20240129' AND T0.INTG_MEMB_NO = '2024463756'
               GROUP BY T0.PRMTNCD
                     , T0.STR_CD
                     , T0.STD_DT
                     , T0.INTG_MEMB_NO    
                     , T0.CMPN_OFFER_NO 
                     , T0.PRMTN_SECTRG_NO 
             ) T0
          LEFT OUTER JOIN D_PRMTN D1 ON T0.PRMTNCD = D1.PRMTNCD
              /* 교환권 관련 매출 */
          LEFT OUTER JOIN TEMP_PRMTN_OFFER_ACTRSLT_01_INIT T1
            ON T0.PRMTNCD           = T1.PRMTNCD
           AND T0.STR_CD            = T1.STR_CD
           AND T0.STD_DT            = T1.STD_DT
           AND T0.INTG_MEMB_NO      = T1.INTG_MEMB_NO
           AND NVL(T0.CMPN_OFFER_NO,0)     = nvl(T1.CMPN_OFFER_NO,0)    
           AND NVL(T0.PRMTN_SECTRG_NO,0)   = nvl(T1.PRMTN_SECTRG_NO,0)
           --AND T1.STD_DT BETWEEN D1.PRMTN_STRT_DT AND TO_CHAR(TO_DATE(D1.PRMTN_END_DT, 'YYYYMMDD')+30, 'YYYYMMDD')
               /* 할인금액 */
          LEFT OUTER JOIN TEMP_PRMTN_OFFER_ACTRSLT_02_INIT T2
            ON T0.PRMTNCD           = T2.PRMTNCD
           AND T0.STR_CD            = T2.STR_CD
           AND T0.STD_DT            = T2.STD_DT
           AND T0.INTG_MEMB_NO      = T2.INTG_MEMB_NO
           AND NVL(T0.CMPN_OFFER_NO,0)     = nvl(T2.CMPN_OFFER_NO,0)    
           AND NVL(T0.PRMTN_SECTRG_NO,0)   = nvl(T2.PRMTN_SECTRG_NO,0)
               /* 사은품증정금액 */
          LEFT OUTER JOIN TEMP_PRMTN_OFFER_ACTRSLT_03_INIT  T3
            ON T0.PRMTNCD           = T3.PRMTNCD
           AND T0.STR_CD            = T3.STR_CD
           AND T0.STD_DT            = T3.STD_DT
           AND T0.INTG_MEMB_NO      = T3.INTG_MEMB_NO
           AND NVL(T0.CMPN_OFFER_NO,0)     = nvl(T3.CMPN_OFFER_NO,0)    
           AND NVL(T0.PRMTN_SECTRG_NO,0)   = nvl(T3.PRMTN_SECTRG_NO,0) 
               /* LDFPAY증정금액-프리LDFPAY제외 */
          LEFT OUTER JOIN TEMP_PRMTN_OFFER_ACTRSLT_04_INIT T4
            ON T0.PRMTNCD           = T4.PRMTNCD
           AND T0.STR_CD            = T4.STR_CD
           AND T0.STD_DT            = T4.STD_DT
           AND T0.INTG_MEMB_NO      = T4.INTG_MEMB_NO
           AND NVL(T0.CMPN_OFFER_NO,0)     = nvl(T4.CMPN_OFFER_NO,0)    
           AND NVL(T0.PRMTN_SECTRG_NO,0)   = nvl(T4.PRMTN_SECTRG_NO,0)
               /* 프리LDFPAY 사용금액 */
         LEFT OUTER JOIN TEMP_PRMTN_OFFER_ACTRSLT_05_INIT T5
            ON T0.PRMTNCD           = T5.PRMTNCD
           AND T0.STR_CD            = T5.STR_CD
           AND T0.STD_DT            = T5.STD_DT
           AND T0.INTG_MEMB_NO      = T5.INTG_MEMB_NO
           AND NVL(T0.CMPN_OFFER_NO,0)     = nvl(T5.CMPN_OFFER_NO,0)    
           AND NVL(T0.PRMTN_SECTRG_NO,0)   = nvl(T5.PRMTN_SECTRG_NO,0)
         GROUP BY T0.PRMTNCD
                , T0.STR_CD
                , T0.STD_DT
                , T0.INTG_CUST_DISTING_NO 
                , T0.INTG_MEMB_NO
                , NVL(COALESCE(T1.CMPN_OFFER_NO,T2.CMPN_OFFER_NO,T3.CMPN_OFFER_NO
                           ,T4.CMPN_OFFER_NO,T5.CMPN_OFFER_NO),0) 
                , NVL(COALESCE(T1.PRMTN_SECTRG_NO,T2.PRMTN_SECTRG_NO,T3.PRMTN_SECTRG_NO
                       ,T4.PRMTN_SECTRG_NO,T5.PRMTN_SECTRG_NO),0) 
     ) T1
  LEFT OUTER JOIN D_MEMB T2 /* D_회원 */
    ON T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
  LEFT OUTER JOIN WL_SL_STR_INFO T3
    ON T1.STR_CD = T3.STR_CD
  LEFT JOIN D_INTG_MEMB T6
    ON T1.INTG_MEMB_NO = T6.INTG_MEMB_NO
  LEFT OUTER JOIN WL_SL_PSPT_RCGNT_NO T4
    ON NVL(T6.PSPT_RCGNT_NO, T2.PSPT_RCGNT_NO) = T4.PSPT_RCGNT_NO   /* 여권인식번호 */
 JOIN (
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
--WHERE T1.PRMTN_SECTRG_NO IS NOT NULL 
 -------------------------------------------------------------------------------------------------------------------       
        
TRUNCATE TABLE FL_MK_PRMTN_OFFER_ACTRSLT

ALTER SESSION ENABLE PARALLEL DML

INSERT /*+ APPEND */ INTO FL_MK_PRMTN_OFFER_ACTRSLT 
SELECT /*+ PARALLEL(4) */   PRMTNCD
, STR_CD
, STD_DT
, INTG_MEMB_NO
, NVL(OFFER_NO,0)
, NVL(PRMTN_SECTRG_NO,0)
, INTG_CUST_DISTING_NO
, INTG_MEMB_YN
, CUST_DISTING_NO
, PSPT_RCGNT_NO
, PSPTNO_NML_YN
, CUST_SALES_DVS_CD
, SL_CHNL_CD
, MEMB_DVS_CD
, VIP_CARD_DVS_CD
, NATLT_CD
, NTV_FORN_DVS_CD
, RESID_STD_NATLT_DVS_CD
, SEX_CD
, AGE_CD
, FRNCTR_RGN_DVS_CD
, INTG_MEMB_GRD_CD
, WDRMBSHIP_YN
, PRMTN_TGTPSN_YN
, LGPRCD
, MDPRCD
, PRMTN_LGCSF_CD
, PRMTN_MDCSF_CD
, CMPN_OFFER_NO
, CUST_CLSF_CD
, GRP_CLSF_CD
, IMGN_INFO_DVS_CD
, IMGN_DT
, DLR_PDCST_SUM_AMT
, WON_PDCST_SUM_AMT
, DLR_EXCH_NSALAMT
, WON_EXCH_NSALAMT
, EXCH_CNT
, DLR_PRMTN_NSALAMT
, WON_PRMTN_NSALAMT
, PRMTN_EXCH_CNT
, DLR_ARSE_NSALAMT
, WON_ARSE_NSALAMT
, DLR_PRMTN_DC_AMT
, WON_PRMTN_DC_AMT
, DC_OURCOM_BDN_AMT
, DC_ALYCO_BDN_AMT
, DC_CNT
, WON_TOT_DC_AMT
, FGF_PRESTAT_AMT
, FGF_PRESTAT_CNT
, PRESTAT_OURCOM_BDN_AMT
, PRESTAT_ALYCO_BDN_AMT
, TOT_PRESTAT_AMT
, PRMTN_PRESTAT_AMT
, PRMTN_CASH_RTRV_AMT
, LDFP_ACMLT_AMT
, LDFP_ACMLT_CNT
, LDFP_OURCOM_BDN_AMT
, LDFP_ALYCO_BDN_AMT
, FREELDFP_USE_AMT
, FREELDFP_USE_CNT
, FREELDFP_OURCOM_BDN_AMT
, FREELDFP_ALYCO_BDN_AMT
, DLR_CROS_ARSE_NSALAMT
, WON_CROS_ARSE_NSALAMT
, OMNI_DLR_NSALAMT
, OMNI_WON_NSALAMT
, OMNI_DLR_EXCH_NSALAMT
, OMNI_WON_EXCH_NSALAMT
, FREELDFP_ACMLT_AMT
, FREELDFP_ACMLT_CNT
, DLR_CROS_EXCH_NSALAMT
, WON_CROS_EXCH_NSALAMT
, LOAD_DTTM
FROM FL_MK_PRMTN_OFFER_ACTRSLT_INIT 





[검증] 
--행사매출/촐할인금액/총증정금액은 구간대에 따라 중복 계산되어 금액이 행사실적에 비해 늘어남
--유발매출의 경우도 일부 차이를 보이는데 행사실적을 다시 적재하면 맞는것으로 보아 오픈시점의 수작업 처리등이 미반영된 상태로 보이며 관련해서 최근일자 일배치 수행을 통해 검증이 필요 
SELECT '1', T1.PRMTNCD, D1.PRMTN_LGCSF_CD 
, SUM(DLR_EXCH_NSALAMT ) AS DLR_EXCH_NSALAMT , SUM(WON_EXCH_NSALAMT ) AS WON_EXCH_NSALAMT , SUM(EXCH_CNT ) AS EXCH_CNT 
, SUM(DLR_PRMTN_NSALAMT ) AS DLR_PRMTN_NSALAMT , SUM(WON_PRMTN_NSALAMT ) AS WON_PRMTN_NSALAMT, SUM(PRMTN_EXCH_CNT ) AS PRMTN_EXCH_CNT 
, SUM(DLR_ARSE_NSALAMT ) AS DLR_ARSE_NSALAMT , SUM(WON_ARSE_NSALAMT ) AS WON_ARSE_NSALAMT 
, SUM(DLR_PRMTN_DC_AMT ) AS DLR_PRMTN_DC_AMT , SUM(WON_PRMTN_DC_AMT ) AS WON_PRMTN_DC_AMT, SUM(DC_OURCOM_BDN_AMT) AS DC_OURCOM_BDN_AMT , SUM(DC_ALYCO_BDN_AMT ) AS DC_ALYCO_BDN_AMT, SUM(DC_CNT ) AS DC_CNT, SUM(WON_TOT_DC_AMT ) AS WON_TOT_DC_AMT 
, SUM(FGF_PRESTAT_AMT ) AS FGF_PRESTAT_AMT , SUM(FGF_PRESTAT_CNT ) AS FGF_PRESTAT_CNT , SUM(PRESTAT_OURCOM_BDN_AMT ) AS PRESTAT_OURCOM_BDN_AMT , SUM(PRESTAT_ALYCO_BDN_AMT ) AS PRESTAT_ALYCO_BDN_AMT , SUM(TOT_PRESTAT_AMT ) AS TOT_PRESTAT_AMT 
, SUM(PRMTN_PRESTAT_AMT ) AS PRMTN_PRESTAT_AMT , SUM(PRMTN_CASH_RTRV_AMT ) AS PRMTN_CASH_RTRV_AMT 
, SUM(LDFP_ACMLT_AMT ) AS LDFP_ACMLT_AMT , SUM(LDFP_ACMLT_CNT ) AS LDFP_ACMLT_CNT , SUM(LDFP_OURCOM_BDN_AMT ) AS LDFP_OURCOM_BDN_AMT , SUM(LDFP_ALYCO_BDN_AMT ) AS LDFP_ALYCO_BDN_AMT 
, SUM(FREELDFP_USE_AMT ) AS FREELDFP_USE_AMT , SUM(FREELDFP_USE_CNT ) AS FREELDFP_USE_CNT , SUM(FREELDFP_OURCOM_BDN_AMT ) AS FREELDFP_OURCOM_BDN_AMT , SUM(FREELDFP_ALYCO_BDN_AMT ) AS FREELDFP_ALYCO_BDN_AMT 
, SUM(DLR_CROS_ARSE_NSALAMT ) AS DLR_CROS_ARSE_NSALAMT , SUM(WON_CROS_ARSE_NSALAMT ) AS WON_CROS_ARSE_NSALAMT 
, SUM(OMNI_DLR_NSALAMT ) AS OMNI_DLR_NSALAMT , SUM(OMNI_WON_NSALAMT ) AS OMNI_WON_NSALAMT , SUM(OMNI_DLR_EXCH_NSALAMT ) AS OMNI_DLR_EXCH_NSALAMT , SUM(OMNI_WON_EXCH_NSALAMT ) AS OMNI_WON_EXCH_NSALAMT 
, SUM(FREELDFP_ACMLT_AMT ) AS FREELDFP_ACMLT_AMT , SUM(FREELDFP_ACMLT_CNT ) AS FREELDFP_ACMLT_CNT 
FROM FL_MK_PRMTN_ACTRSLT T1
JOIN D_PRMTN D1 ON T1.PRMTNCD = D1.PRMTNCD
WHERE STD_DT BETWEEN  '20231201' AND '20231231'
GROUP BY T1.PRMTNCD, D1.PRMTN_LGCSF_CD 
UNION ALL
SELECT '2', T1.PRMTNCD, D1.PRMTN_LGCSF_CD 
, SUM(DLR_EXCH_NSALAMT ) AS DLR_EXCH_NSALAMT , SUM(WON_EXCH_NSALAMT ) AS WON_EXCH_NSALAMT , SUM(EXCH_CNT ) AS EXCH_CNT 
, SUM(DLR_PRMTN_NSALAMT ) AS DLR_PRMTN_NSALAMT , SUM(WON_PRMTN_NSALAMT ) AS WON_PRMTN_NSALAMT, SUM(PRMTN_EXCH_CNT ) AS PRMTN_EXCH_CNT 
, SUM(DLR_ARSE_NSALAMT ) AS DLR_ARSE_NSALAMT , SUM(WON_ARSE_NSALAMT ) AS WON_ARSE_NSALAMT 
, SUM(DLR_PRMTN_DC_AMT ) AS DLR_PRMTN_DC_AMT , SUM(WON_PRMTN_DC_AMT ) AS WON_PRMTN_DC_AMT, SUM(DC_OURCOM_BDN_AMT) AS DC_OURCOM_BDN_AMT , SUM(DC_ALYCO_BDN_AMT ) AS DC_ALYCO_BDN_AMT, SUM(DC_CNT ) AS DC_CNT, SUM(WON_TOT_DC_AMT ) AS WON_TOT_DC_AMT 
, SUM(FGF_PRESTAT_AMT ) AS FGF_PRESTAT_AMT , SUM(FGF_PRESTAT_CNT ) AS FGF_PRESTAT_CNT , SUM(PRESTAT_OURCOM_BDN_AMT ) AS PRESTAT_OURCOM_BDN_AMT , SUM(PRESTAT_ALYCO_BDN_AMT ) AS PRESTAT_ALYCO_BDN_AMT , SUM(TOT_PRESTAT_AMT ) AS TOT_PRESTAT_AMT 
, SUM(PRMTN_PRESTAT_AMT ) AS PRMTN_PRESTAT_AMT , SUM(PRMTN_CASH_RTRV_AMT ) AS PRMTN_CASH_RTRV_AMT 
, SUM(LDFP_ACMLT_AMT ) AS LDFP_ACMLT_AMT , SUM(LDFP_ACMLT_CNT ) AS LDFP_ACMLT_CNT , SUM(LDFP_OURCOM_BDN_AMT ) AS LDFP_OURCOM_BDN_AMT , SUM(LDFP_ALYCO_BDN_AMT ) AS LDFP_ALYCO_BDN_AMT 
, SUM(FREELDFP_USE_AMT ) AS FREELDFP_USE_AMT , SUM(FREELDFP_USE_CNT ) AS FREELDFP_USE_CNT , SUM(FREELDFP_OURCOM_BDN_AMT ) AS FREELDFP_OURCOM_BDN_AMT , SUM(FREELDFP_ALYCO_BDN_AMT ) AS FREELDFP_ALYCO_BDN_AMT 
, SUM(DLR_CROS_ARSE_NSALAMT ) AS DLR_CROS_ARSE_NSALAMT , SUM(WON_CROS_ARSE_NSALAMT ) AS WON_CROS_ARSE_NSALAMT 
, SUM(OMNI_DLR_NSALAMT ) AS OMNI_DLR_NSALAMT , SUM(OMNI_WON_NSALAMT ) AS OMNI_WON_NSALAMT , SUM(OMNI_DLR_EXCH_NSALAMT ) AS OMNI_DLR_EXCH_NSALAMT , SUM(OMNI_WON_EXCH_NSALAMT ) AS OMNI_WON_EXCH_NSALAMT 
, SUM(FREELDFP_ACMLT_AMT ) AS FREELDFP_ACMLT_AMT , SUM(FREELDFP_ACMLT_CNT ) AS FREELDFP_ACMLT_CNT 
FROM FL_MK_PRMTN_OFFER_ACTRSLT_INIT T1
JOIN D_PRMTN D1 ON T1.PRMTNCD = D1.PRMTNCD
WHERE STD_DT  BETWEEN  '20231201' AND '20231231'
GROUP BY T1.PRMTNCD, D1.PRMTN_LGCSF_CD 
ORDER BY 2,3,1

        
---------------------- ---------------------------------------------------------------------------       
*****   오프라인 오파 머지 업데이트 작업 (테이블 복제 후 작업) 
EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

/* LDFPAY유발매출 */
MERGE /*+ APPEND PARALLEL(4) */
 INTO FL_MK_PRMTN_EXCH_SL_TEST T1
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
                           , SUM(CASE WHEN T1.LDFP_RCPDSBS_TYPE_CD in('21','26') THEN T1.LDFP_USE_AMT ELSE 0 END) OVER(PARTITION BY T1.PRMTNCD, T1.STR_CD, T1.LDFP_HIST_HAPN_DT, T1.INTG_MEMB_NO, T1.CMPN_OFFER_NO , T1.PRMTN_SECTRG_NO   )  AS RETR
                        FROM FL_MK_LDFP_ACMLT_USE_PTCLS  T1 /* LDFPAY적립사용내역 */
                       INNER JOIN D_PRMTN T3 /* D_행사 */
                          ON T1.PRMTNCD        = T3.PRMTNCD
                         AND T3.PRMTN_LGCSF_CD = '005'  /* LDFPAY */
                         AND T1.LDFP_HIST_HAPN_DT BETWEEN T3.PRMTN_STRT_DT AND (CASE WHEN T3.PRMTN_END_DT < '99991201'
                                                                                     THEN TO_CHAR(TO_DATE(T3.PRMTN_END_DT,'YYYYMMDD') + 30,'YYYYMMDD')
                                                                                     ELSE T3.PRMTN_END_DT END)
                       WHERE T1.LDFP_ACTI_YN     = 'Y'
                         AND T1.LDFP_NO_STAT_CD <> '00'
                         AND T1.ACMLT_USE_DVS_CD = '2'  /* 사용 */
                         AND T1.LDFP_RCPDSBS_TYPE_CD in('21','26')  -- AND EXCH_NO = '90805519004371'
                      ) T1
                 WHERE RETR <> 0
                 GROUP BY PRMTNCD
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
          , T1.CMPN_OFFER_NO    = T2.CMPN_OFFER_NO
          , T1.PRMTN_SECTRG_NO  = T2.PRMTN_SECTRG_NO
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