/**********  UQ_CREATE_01  ****************************************************/


EXEC PR_CHK_DROP_TABLE('FE_MK_MIDPROMO_TEMP');

CREATE TABLE FE_MK_MIDPROMO_TEMP NOLOGGING AS
SELECT /*+ PARALLEL(4) */
       T1.MDPRCD
     , T1.STD_DT
     , T1.ONLN_ORD_NO
     , T1.INTG_CUST_DISTING_NO
     , T1.CUST_DISTING_NO
     , T1.INTG_MEMB_NO
     , T1.LANG_CD
     , T1.DVIC_CD
     , T1.NATLT_CD
     , T1.EXCH_PRMTN_APLY_YN
     , T1.OMNI_ORD_YN
     , T1.ORD_MBSSYS_DVS_CD
     , T1.ONLN_MEMB_NO
     , T1.INTG_MEMB_GRD_CD
     , T1.IMGN_INFO_DVS_CD /* 출입국정보구분코드 1:출국 */
     , T1.IMGN_DT          /* 출입국일자 */
     , T1.LGPRCD
     , T1.PRMTN_STRT_DT
     , T1.PRMTN_END_DT
     , T1.PRMTN_LGCSF_CD
     , T1.DLR_TOT_NSALAMT
     , T1.WON_TOT_NSALAMT
     , T2.DLR_ACMLTMN_ARSE_NSALAMT
     , T2.WON_ACMLTMN_ARSE_NSALAMT
     , T2.DLR_LDFP_ARSE_NSALAMT
     , T2.WON_LDFP_ARSE_NSALAMT
     , T1.DLR_CROS_NSALAMT
     , T1.WON_CROS_NSALAMT
  FROM
     (
       SELECT T1.MDPRCD
            , T1.STD_DT
            , T1.ONLN_ORD_NO
            , T1.INTG_CUST_DISTING_NO
            , T1.CUST_DISTING_NO
            , T1.INTG_MEMB_NO
            , T1.LANG_CD
            , T1.DVIC_CD
            , T1.NATLT_CD
            , T1.EXCH_PRMTN_APLY_YN
            , T1.OMNI_ORD_YN
            , T1.ORD_MBSSYS_DVS_CD
            , T1.ONLN_MEMB_NO
            , T1.INTG_MEMB_GRD_CD
            , T1.IMGN_INFO_DVS_CD /* 출입국정보구분코드 1:출국 */
            , T1.IMGN_DT          /* 출입국일자 */
            , T1.LGPRCD
            , T3.PRMTN_STRT_DT
            , T3.PRMTN_END_DT
            , T1.PRMTN_LGCSF_CD
            , T1.DLR_TOT_NSALAMT
            , T1.WON_TOT_NSALAMT
            , T1.DLR_ACMLTMN_ARSE_NSALAMT
            , T1.WON_ACMLTMN_ARSE_NSALAMT
            , T1.DLR_LDFP_ARSE_NSALAMT
            , T1.WON_LDFP_ARSE_NSALAMT
            , T1.DLR_CROS_NSALAMT
            , T1.WON_CROS_NSALAMT
            , ROW_NUMBER() OVER(PARTITION BY T1.MDPRCD, T1.STD_DT, T1.INTG_MEMB_NO, T1.LANG_CD, T1.DVIC_CD, T1.ONLN_ORD_NO ORDER BY T1.EXCH_PRMTN_APLY_YN DESC, T1.PRMTNCD) AS RN
         FROM FE_MK_PRMTN_ORD_SL T1 /* FE_MK_행사주문판매 */
        /*-- 변경적재 모수 -------------------------*/
        INNER JOIN FE_MK_ONLN_PRMTN_CHNG_TEMP  T2 /* 변경모수 */
           ON T1.PRMTNCD      = T2.PRMTNCD
          AND T1.STD_DT       = T2.STD_DT
          AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
         ---------------------------------------
        INNER JOIN D_PRMTN T3
           ON T1.PRMTNCD = T3.PRMTNCD
        WHERE T1.MDPRCD IS NOT NULL
     ) T1
  LEFT OUTER JOIN (
       SELECT T1.MDPRCD
            , T1.STD_DT
            , T1.ONLN_ORD_NO
            , T1.INTG_MEMB_NO
            , T1.LANG_CD
            , T1.DVIC_CD
            , T1.DLR_ACMLTMN_ARSE_NSALAMT
            , T1.WON_ACMLTMN_ARSE_NSALAMT
            , T1.DLR_LDFP_ARSE_NSALAMT
            , T1.WON_LDFP_ARSE_NSALAMT
            , ROW_NUMBER() OVER(PARTITION BY T1.MDPRCD, T1.STD_DT, T1.INTG_MEMB_NO, T1.LANG_CD, T1.DVIC_CD, T1.ONLN_ORD_NO ORDER BY T1.PRMTNCD) AS RNUM
         FROM FE_MK_PRMTN_ORD_SL T1 /* FE_MK_행사주문판매 */
        /*-- 변경적재 모수 -------------------------*/
        INNER JOIN FE_MK_ONLN_PRMTN_CHNG_TEMP  T2 /* 변경모수 */
           ON T1.PRMTNCD      = T2.PRMTNCD
          AND T1.STD_DT       = T2.STD_DT
          AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
         ---------------------------------------
        WHERE T1.DLR_ACMLTMN_ARSE_NSALAMT+T1.DLR_LDFP_ARSE_NSALAMT <> 0
    ) T2
     ON T1.MDPRCD = T2.MDPRCD
    AND T1.STD_DT = T2.STD_DT
    AND T1.ONLN_ORD_NO = T2.ONLN_ORD_NO
    AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
    AND T1.LANG_CD = T2.LANG_CD
    AND T2.RNUM = 1
  WHERE T1.RN = 1
;





/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FE_MK_MIDPROMO_ACTRSLT T1
 WHERE EXISTS (
                SELECT 1
                  FROM (
                       SELECT DISTINCT STD_DT
                            , MDPRCD
                            , INTG_MEMB_NO
                         FROM FE_MK_MIDPROMO_TEMP /* 변경적재_TEMP   */
                       ) T0
                 WHERE T1.STD_DT        = T0.STD_DT
                   AND T1.MDPRCD        = T0.MDPRCD
                   AND T1.INTG_MEMB_NO  = T0.INTG_MEMB_NO
              ) ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND PARALLEL(T 4) */
  INTO FE_MK_MIDPROMO_ACTRSLT T
SELECT /*+ USE_HASH(T1) PARALLEL(4) */
       T1.STD_DT                                                          /* 기준일자               */
     , T1.MDPRCD                                                          /* 중행사코드             */
     , T1.INTG_MEMB_NO                    AS INTG_MEMB_NO                 /* 통합회원번호           */
     , NVL(T1.LANG_CD              ,'z')  AS LANG_CD                      /* 언어코드               */
     , NVL(T1.DVIC_CD              ,'z')  AS DVIC_CD                      /* 디바이스코드           */
     , MAX(T1.CUST_DISTING_NO)            AS CUST_DISTING_NO              /* 고객식별번호           */
     , MAX(CASE WHEN T1.INTG_MEMB_NO IS NOT NULL THEN 'Y' ELSE 'N' END) AS INTG_MEMB_YN /* 통합회원여부 */
     , MAX(T1.INTG_CUST_DISTING_NO     )  AS INTG_CUST_DISTING_NO         /* 통합고객식별번호       */
     , MAX(NVL(T1.NATLT_CD         ,'z')) AS NATLT_CD                     /* 국적코드               */
     , MAX(CASE WHEN T1.ORD_MBSSYS_DVS_CD = '04' THEN 'N' ELSE 'Y' END) AS MEMB_YN  /* 회원여부 */
     , MAX(T1.ONLN_MEMB_NO)               AS ONLN_MEMB_NO
     , MAX(NVL(T1.INTG_MEMB_GRD_CD     ,'z')) AS INTG_MEMB_GRD_CD         /* 통합회원등급코드       */
     , MAX(NVL(T1.IMGN_INFO_DVS_CD     ,'z')) AS IMGN_INFO_DVS_CD         /* 출입국정보구분코드 1:출국 */
     , MAX(T1.IMGN_DT                       ) AS IMGN_DT                  /* 출입국일자 */
     , MAX(NVL(T7.LGPRCD               ,'z')) AS LGPRCD                   /* 대행사코드             */
     , SUM(NVL(T2.DLR_PDCST_SUM_AMT      ,0)) AS DLR_PDCST_SUM_AMT        /* 달러원가합계금액       */
     , SUM(NVL(T2.WON_PDCST_SUM_AMT      ,0)) AS WON_PDCST_SUM_AMT        /* 원화원가합계금액       */
     , SUM(NVL(T2.DLR_NSALAMT            ,0)) AS DLR_NSALAMT              /* 달러순매출액           */
     , SUM(NVL(T2.WON_NSALAMT            ,0)) AS WON_NSALAMT              /* 원화순매출액           */
     , SUM(NVL(T2.DLR_PRMTN_NSALAMT      ,0)) AS DLR_PRMTN_NSALAMT        /* 달러행사순매출액       */
     , SUM(NVL(T2.WON_PRMTN_NSALAMT      ,0)) AS WON_PRMTN_NSALAMT        /* 원화행사순매출액       */
     , SUM(NVL(T3.DC_CNT                 ,0)) AS DC_CNT                   /* 할인건수               */
     , SUM(NVL(T3.DLR_PRMTN_DC_AMT       ,0)) AS DLR_PRMTN_DC_AMT         /* 달러행사할인금액       */
     , SUM(NVL(T3.WON_PRMTN_DC_AMT       ,0)) AS WON_PRMTN_DC_AMT         /* 원화행사할인금액       */
     , SUM(NVL(T3.DC_OURCOM_BDN_AMT      ,0)) AS DC_OURCOM_BDN_AMT        /* 할인당사부담금액       */
     , SUM(NVL(T3.DC_ALYCO_BDN_AMT       ,0)) AS DC_ALYCO_BDN_AMT         /* 할인제휴사부담금액     */
     , SUM(NVL(T3.ACMLTMN_OFFR_CNT       ,0)) AS ACMLTMN_OFFR_CNT         /* 적립금제공건수         */
     , SUM(NVL(T3.DLR_ACMLTMN_OFFR_AMT   ,0)) AS DLR_ACMLTMN_OFFR_AMT     /* 달러적립금제공금액     */
     , SUM(NVL(T3.WON_ACMLTMN_OFFR_AMT   ,0)) AS WON_ACMLTMN_OFFR_AMT     /* 원화적립금제공금액     */
     , SUM(NVL(T3.ACMLTMN_USE_CNT        ,0)) AS ACMLTMN_USE_CNT          /* 적립금사용건수         */
     , SUM(NVL(T3.DLR_ACMLTMN_PYF_AMT    ,0)) AS DLR_ACMLTMN_PYF_AMT      /* 달러적립금결제금액     */
     , SUM(NVL(T3.WON_ACMLTMN_PYF_AMT    ,0)) AS WON_ACMLTMN_PYF_AMT      /* 원화적립금결제금액     */
     , SUM(NVL(T3.ACMLTMN_OURCOM_BDN_AMT ,0)) AS ACMLTMN_OURCOM_BDN_AMT   /* 적립금당사부담금액     */
     , SUM(NVL(T3.ACMLTMN_ALYCO_BDN_AMT  ,0)) AS ACMLTMN_ALYCO_BDN_AMT    /* 적립금제휴사부담금액   */
     , SUM(NVL(T2.DLR_ACMLTMN_USE_NSALAMT,0) + NVL(T2.DLR_ACMLTMN_ARSE_NSALAMT,0)) AS DLR_ACMLTMN_USE_NSALAMT  /* 달러적립금사용순매출액  */
     , SUM(NVL(T2.WON_ACMLTMN_USE_NSALAMT,0) + NVL(T2.WON_ACMLTMN_ARSE_NSALAMT,0)) AS WON_ACMLTMN_USE_NSALAMT  /* 원화적립금사용순매출액  */
     , SUM(NVL(T3.LDFP_ACMLT_CNT         ,0)) AS LDFP_ACMLT_CNT           /* LDFPAY적립건수         */
     , SUM(NVL(T3.DLR_LDFP_ACMLT_AMT     ,0)) AS DLR_LDFP_ACMLT_AMT       /* 달러LDFPAY적립금액     */
     , SUM(NVL(T3.WON_LDFP_ACMLT_AMT     ,0)) AS WON_LDFP_ACMLT_AMT       /* 원화LDFPAY적립금액     */
     , SUM(NVL(T3.LDFP_OURCOM_BDN_AMT    ,0)) AS LDFP_OURCOM_BDN_AMT      /* LDFPAY당사부담금액     */
     , SUM(NVL(T3.LDFP_ALYCO_BDN_AMT     ,0)) AS LDFP_ALYCO_BDN_AMT       /* LDFPAY제휴사부담금액   */
     , SUM(NVL(T2.DLR_ACMLTMN_USE_NSALAMT,0) + NVL(T2.DLR_ACMLTMN_ARSE_NSALAMT,0) + NVL(T2.DLR_LDFP_ARSE_NSALAMT,0)) AS DLR_ARSE_NSALAMT  /* 달러유발순매출액      */
     , SUM(NVL(T2.WON_ACMLTMN_USE_NSALAMT,0) + NVL(T2.WON_ACMLTMN_ARSE_NSALAMT,0) + NVL(T2.WON_LDFP_ARSE_NSALAMT,0)) AS WON_ARSE_NSALAMT  /* 원화유발순매출액      */
     , SUM(NVL(T2.DLR_LDFP_ARSE_NSALAMT  ,0)) AS DLR_LDFP_ARSE_NSALAMT    /* 달러LDFPAY유발순매출액 */
     , SUM(NVL(T2.WON_LDFP_ARSE_NSALAMT  ,0)) AS WON_LDFP_ARSE_NSALAMT    /* 원화LDFPAY유발순매출액 */
     , SUM(NVL(T2.OMNI_DLR_NSALAMT       ,0)) AS OMNI_DLR_NSALAMT         /* 옴니달러순매출액       */
     , SUM(NVL(T2.OMNI_WON_NSALAMT       ,0)) AS OMNI_WON_NSALAMT         /* 옴니원화순매출액       */
     , SUM(NVL(T2.OMNI_DLR_EXCH_NSALAMT  ,0)) AS OMNI_DLR_EXCH_NSALAMT    /* 옴니달러교환권순매출액 */
     , SUM(NVL(T2.OMNI_WON_EXCH_NSALAMT  ,0)) AS OMNI_WON_EXCH_NSALAMT    /* 옴니원화교환권순매출액 */
     , SUM(NVL(T2.DLR_CROS_NSALAMT       ,0)) AS DLR_CROS_NSALAMT         /* 달러교차순매출액       */
     , SUM(NVL(T2.WON_CROS_NSALAMT       ,0)) AS WON_CROS_NSALAMT         /* 원화교차순매출액       */
     , SYSDATE                                AS LOAD_DTTM                /* 적재일시               */
     , 0                                      AS DLR_CROS_EXCH_NSALAMT    /*달러교차교환권순매출액  */
     , 0                                      AS WON_CROS_EXCH_NSALAMT    /*원화교차교환권순매출액  */
  FROM
     (
       SELECT /*+ PARALLEL(4) */
              STD_DT
            , MDPRCD
            , INTG_MEMB_NO
            , LANG_CD
            , DVIC_CD
            , MAX(INTG_CUST_DISTING_NO) AS INTG_CUST_DISTING_NO
            , MAX(CUST_DISTING_NO     ) AS CUST_DISTING_NO
            , MAX(NATLT_CD            ) AS NATLT_CD
            , MAX(ORD_MBSSYS_DVS_CD   ) AS ORD_MBSSYS_DVS_CD
            , MAX(ONLN_MEMB_NO        ) AS ONLN_MEMB_NO
            , MAX(INTG_MEMB_GRD_CD    ) AS INTG_MEMB_GRD_CD
            , MAX(IMGN_INFO_DVS_CD    ) AS IMGN_INFO_DVS_CD  /* 출입국정보구분코드 1:출국 */
            , MAX(IMGN_DT             ) AS IMGN_DT           /* 출입국일자 */
         FROM FE_MK_MIDPROMO_TEMP /* 변경적재temp   */
        GROUP BY STD_DT
            , MDPRCD
            , INTG_MEMB_NO
            , LANG_CD
            , DVIC_CD
     ) T1
  LEFT OUTER JOIN (
     /* 주문관련 */
     SELECT  /*+ PARALLEL(4) */
            T1.MDPRCD
          , T1.STD_DT
          , T1.INTG_MEMB_NO
          , T1.LANG_CD
          , T1.DVIC_CD
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T2.DLR_PDCST ELSE 0 END)       AS DLR_PDCST_SUM_AMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T2.WON_PDCST ELSE 0 END)       AS WON_PDCST_SUM_AMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.DLR_TOT_NSALAMT ELSE 0 END) AS DLR_NSALAMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.WON_TOT_NSALAMT ELSE 0 END) AS WON_NSALAMT
          , SUM(T1.DLR_TOT_NSALAMT) AS DLR_PRMTN_NSALAMT
          , SUM(T1.WON_TOT_NSALAMT) AS WON_PRMTN_NSALAMT
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' THEN T1.DLR_TOT_NSALAMT ELSE 0 END) AS OMNI_DLR_NSALAMT                 /* 옴니달러순매출액 */
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' THEN T1.WON_TOT_NSALAMT ELSE 0 END) AS OMNI_WON_NSALAMT                 /* 옴니원화순매출액 */
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' AND T1.EXCH_PRMTN_APLY_YN ='Y' THEN T1.DLR_TOT_NSALAMT ELSE 0 END) AS OMNI_DLR_EXCH_NSALAMT        /* 옴니달러교환권순매출액 */
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' AND T1.EXCH_PRMTN_APLY_YN ='Y' THEN T1.WON_TOT_NSALAMT ELSE 0 END) AS OMNI_WON_EXCH_NSALAMT        /* 옴니원화교환권순매출액 */
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' AND T1.PRMTN_LGCSF_CD = '007' THEN T1.DLR_TOT_NSALAMT ELSE 0 END) AS DLR_ACMLTMN_USE_NSALAMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' AND T1.PRMTN_LGCSF_CD = '007' THEN T1.WON_TOT_NSALAMT ELSE 0 END) AS WON_ACMLTMN_USE_NSALAMT
          , SUM(T1.DLR_ACMLTMN_ARSE_NSALAMT)       AS DLR_ACMLTMN_ARSE_NSALAMT
          , SUM(T1.WON_ACMLTMN_ARSE_NSALAMT)       AS WON_ACMLTMN_ARSE_NSALAMT
          , SUM(T1.DLR_LDFP_ARSE_NSALAMT   )       AS DLR_LDFP_ARSE_NSALAMT    /* 달러LDFPAY유발순매출액 */
          , SUM(T1.WON_LDFP_ARSE_NSALAMT   )       AS WON_LDFP_ARSE_NSALAMT    /* 원화LDFPAY유발순매출액 */
          , SUM(T1.DLR_CROS_NSALAMT        )       AS DLR_CROS_NSALAMT         /* 달러교차순매출액       */
          , SUM(T1.WON_CROS_NSALAMT        )       AS WON_CROS_NSALAMT         /* 원화교차순매출액       */
       FROM FE_MK_MIDPROMO_TEMP T1         /* 변경적재temp */
       LEFT OUTER JOIN
          (
            SELECT /*+ USE_HASH(T1) PARALLEL(4) */
                   T1.STD_DT
                 , T1.ONLN_ORD_NO
                 , T1.MDPRCD
                 , SUM(T4.DLR_PDCST*T3.SALES_SIGN*T3.ORD_QTY) AS DLR_PDCST
                 , SUM(T4.WON_PDCST*T3.SALES_SIGN*T3.ORD_QTY) AS WON_PDCST
              FROM FE_MK_MIDPROMO_TEMP T1    /* 변경적재temp   */
             INNER JOIN FE_SL_ORD_PROD T3    /* FE_SL_주문상품 */
                ON T1.ONLN_ORD_NO     = T3.ONLN_ORD_NO
               AND T1.STD_DT          = T3.STD_DT
               AND T1.DLR_TOT_NSALAMT > 0
             INNER JOIN WL_IO_DD_PRICE T4  /* WL_IO_일수불가격 */
                ON T3.STD_DT = T4.RCPDSBS_DT
               AND T3.PRDCD  = T4.PRDCD
               AND T3.STR_CD = T4.STR_CD
             GROUP BY T1.STD_DT
                    , T1.ONLN_ORD_NO
                    , T1.MDPRCD
          ) T2
         ON T1.ONLN_ORD_NO = T2.ONLN_ORD_NO
        AND T1.STD_DT      = T2.STD_DT
        AND T1.MDPRCD      = T2.MDPRCD
      GROUP BY T1.MDPRCD
             , T1.STD_DT
             , T1.INTG_MEMB_NO
             , T1.LANG_CD
             , T1.DVIC_CD
     ) T2
   ON T1.MDPRCD       = T2.MDPRCD
  AND T1.STD_DT       = T2.STD_DT
  AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
  AND T1.LANG_CD      = T2.LANG_CD
  AND T1.DVIC_CD      = T2.DVIC_CD
  LEFT OUTER JOIN (
     /* 할인금액 : offer */
     SELECT /*+ PARALLEL(4) */
            T2.MDPRCD
          , T2.STD_DT
          , T2.INTG_MEMB_NO
          , T2.LANG_CD
          , T2.DVIC_CD
          , SUM(T2.DC_CNT                ) AS DC_CNT                 /* 할인건수 */
          , SUM(T2.DLR_PRMTN_DC_AMT      ) AS DLR_PRMTN_DC_AMT       /* 달러행사할인금액 */
          , SUM(T2.WON_PRMTN_DC_AMT      ) AS WON_PRMTN_DC_AMT       /* 원화행사할인금액 */
          , SUM(T2.DC_OURCOM_BDN_AMT     ) AS DC_OURCOM_BDN_AMT      /* 할인당사부담금액 */
          , SUM(T2.DC_ALYCO_BDN_AMT      ) AS DC_ALYCO_BDN_AMT       /* 할인제휴사부담금액 */
          , SUM(T2.ACMLTMN_OFFR_CNT      ) AS ACMLTMN_OFFR_CNT       /* 적립금제공건수*/
          , SUM(T2.DLR_ACMLTMN_OFFR_AMT  ) AS DLR_ACMLTMN_OFFR_AMT   /* 달러적립금제공금액*/
          , SUM(T2.WON_ACMLTMN_OFFR_AMT  ) AS WON_ACMLTMN_OFFR_AMT   /* 원화적립금제공금액*/
          , SUM(T2.ACMLTMN_USE_CNT       ) AS ACMLTMN_USE_CNT        /* 적립금사용건수 */
          , SUM(T2.DLR_ACMLTMN_PYF_AMT   ) AS DLR_ACMLTMN_PYF_AMT    /* 달러적립금결제금액 */
          , SUM(T2.WON_ACMLTMN_PYF_AMT   ) AS WON_ACMLTMN_PYF_AMT    /* 원화적립금결제금액*/
          , SUM(T2.ACMLTMN_OURCOM_BDN_AMT) AS ACMLTMN_OURCOM_BDN_AMT /* 적립금당사부담금액 */
          , SUM(T2.ACMLTMN_ALYCO_BDN_AMT ) AS ACMLTMN_ALYCO_BDN_AMT  /* 적립금제휴사부담금액 */
          , SUM(T2.LDFP_ACMLT_CNT        ) AS LDFP_ACMLT_CNT         /* LDFPAY적립건수 */
          , SUM(T2.DLR_LDFP_ACMLT_AMT    ) AS DLR_LDFP_ACMLT_AMT     /* 달러LDFPAY적립금액*/
          , SUM(T2.WON_LDFP_ACMLT_AMT    ) AS WON_LDFP_ACMLT_AMT     /* 원화LDFPAY적립금액*/
          , SUM(T2.LDFP_OURCOM_BDN_AMT   ) AS LDFP_OURCOM_BDN_AMT    /* LDFPAY당사부담금액 */
          , SUM(T2.LDFP_ALYCO_BDN_AMT    ) AS LDFP_ALYCO_BDN_AMT     /* LDFPAY제휴사부담금액 */
       FROM FE_MK_PRMTN_ACTRSLT T2  /* FE_MK_행사실적 */
      INNER JOIN
          (
            SELECT DISTINCT MDPRCD
                 , STD_DT
                 , INTG_MEMB_NO
              FROM FE_MK_MIDPROMO_TEMP /* 변경적재temp   */
          ) T3  /* 변경적재 대상 */
         ON T2.MDPRCD       = T3.MDPRCD
        AND T2.STD_DT       = T3.STD_DT
        AND T2.INTG_MEMB_NO = T3.INTG_MEMB_NO
      GROUP BY T2.MDPRCD
             , T2.STD_DT
             , T2.INTG_MEMB_NO
             , T2.LANG_CD
             , T2.DVIC_CD
     ) T3
    ON T1.MDPRCD       = T3.MDPRCD
   AND T1.STD_DT       = T3.STD_DT
   AND T1.INTG_MEMB_NO = T3.INTG_MEMB_NO
   AND T1.LANG_CD      = T3.LANG_CD
   AND T1.DVIC_CD      = T3.DVIC_CD
 INNER JOIN D_MIDPROMO  T7
    ON T1.MDPRCD       = T7.MDPRCD
 INNER JOIN D_LGPROMO T8
    ON T7.LGPRCD =  T8.LGPRCD
   AND T8.PRMTN_ACTRSLT_OBJ_XCLUD_YN = 'N'  /* 행사실적대상제외여부 */
 GROUP BY T1.STD_DT
        , T1.MDPRCD
        , T1.INTG_MEMB_NO
        , NVL(T1.LANG_CD, 'z')
        , NVL(T1.DVIC_CD, 'z')
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_MERGE_01  *****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

MERGE /*+ PARALLEL(4) */
 INTO FE_MK_MIDPROMO_ACTRSLT T1
USING (SELECT T2.MDPRCD
            , T2.STD_DT
            , T2.INTG_MEMB_NO
            , T2.LANG_CD
            , T2.DVIC_CD
            , T2.ONLN_MEMB_NO
            , T2.DLR_NSALAMT
            , T2.WON_NSALAMT
         FROM FE_MK_MIDPROMO_ACTRSLT   T2
        INNER JOIN
            ( SELECT DISTINCT
                     T2.MDPRCD
                   -- , T3.LANG_CD
                   -- , T3.DVIC_CD
                   , T3.INTG_MEMB_NO
                   , T1.ONLN_MEMB_NO
                FROM WL_Z_CAMP_CELL_CUST T1
                JOIN D_PRMTN T2
                  ON T1.CAMP_ID            = T2.PRMTNCD
                 AND T2.INTG_MEMB_PRMTN_YN = 'Y'
                JOIN (SELECT PRMTNCD
                           , LANG_CD
                           , DVIC_CD
                           , ONLN_MEMB_NO AS CHNG_ONLN_MEMB_NO
                           , INTG_MEMB_NO
                        FROM FE_MK_PRMTN_ORD_TEMP
                     ) T3
                  ON T1.CAMP_ID      = T3.PRMTNCD
                 AND T1.ONLN_MEMB_NO = T3.CHNG_ONLN_MEMB_NO
               WHERE NVL(TRIM(T1.SEND_MEMB_CD),T2.CHNL_TYPE_CD) <> T2.CHNL_TYPE_CD
            ) T3
           ON T2.MDPRCD       = T3.MDPRCD
          AND T2.ONLN_MEMB_NO = T3.ONLN_MEMB_NO
      ) T2
   ON (T1.MDPRCD        = T2.MDPRCD
  AND  T1.STD_DT        = T2.STD_DT
  AND  T1.INTG_MEMB_NO  = T2.INTG_MEMB_NO)
 WHEN MATCHED THEN
      UPDATE
         SET T1.DLR_CROS_EXCH_NSALAMT = NVL(T2.DLR_NSALAMT,0)
           , T1.WON_CROS_EXCH_NSALAMT = NVL(T2.WON_NSALAMT,0)
           , T1.LOAD_DTTM             = SYSDATE
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;