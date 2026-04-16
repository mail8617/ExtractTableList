/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM FE_MK_PRMTN_ACTRSLT T1
 WHERE EXISTS (
               SELECT 1
                 FROM FE_MK_ONLN_PRMTN_CHNG_TEMP T2
                WHERE T1.STD_DT       = T2.STD_DT
                  AND T1.PRMTNCD      = T2.PRMTNCD
                  AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
              ) ;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND */
  INTO FE_MK_PRMTN_ACTRSLT T
SELECT /*+ USE_HASH(T1 T2 T3 T5 T4) PARALLEL(4) */
       T1.STD_DT                                                          /* 기준일자               */
     , T1.PRMTNCD                                                         /* 행사코드               */
     , T1.INTG_MEMB_NO                                                    /* 통합회원번호           */
     , NVL(T1.LANG_CD                  ,'z')  AS LANG_CD                  /* 언어코드               */
     , NVL(T1.DVIC_CD                  ,'z')  AS DVIC_CD                  /* 디바이스코드           */
     , MAX(T1.CUST_DISTING_NO)                AS CUST_DISTING_NO          /* 고객식별번호           */
     , MAX(CASE WHEN T1.INTG_MEMB_NO IS NOT NULL THEN 'Y' ELSE 'N' END) AS INTG_MEMB_YN /*통합회원여부 */
     , MAX(T1.INTG_CUST_DISTING_NO)           AS INTG_CUST_DISTING_NO     /* 통합고객식별번호       */
     , MAX(NVL(T1.NATLT_CD             ,'z')) AS NATLT_CD                 /* 국적코드               */
    /* , MAX(NVL(T99.RNKH_NATLT_CD,'z'))        AS RNKH_NATLT_CD        */    /*상위국적코드*/
     , MAX(CASE WHEN T1.ORD_MBSSYS_DVS_CD = '04' THEN 'N' ELSE 'Y' END) AS MEMB_YN  /* 회원여부     */
     , MAX(T1.ONLN_MEMB_NO)                   AS ONLN_MEMB_NO
     , MAX(NVL(T1.INTG_MEMB_GRD_CD     ,'z')) AS INTG_MEMB_GRD_CD         /* 통합회원등급코드       */
     , MAX(NVL(T1.IMGN_INFO_DVS_CD     ,'z')) AS IMGN_INFO_DVS_CD         /* 출입국정보구분코드 1:출국 */
     , MAX(T1.IMGN_DT                       ) AS IMGN_DT                  /* 출입국일자             */
     , MAX(NVL(T9.LGPRCD               ,'z')) AS LGPRCD                   /* 대행사코드             */
     , MAX(NVL(T9.MDPRCD               ,'z')) AS MDPRCD                   /* 중행사코드             */
     , MAX(NVL(T9.PRMTN_LGCSF_CD       ,'z')) AS PRMTN_LGCSF_CD           /* 행사대분류코드         */
     , MAX(NVL(T9.PRMTN_MDCSF_CD       ,'z')) AS PRMTN_MDCSF_CD           /* 행사중분류코드         */
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
     , SUM(NVL(T5.ACMLTMN_OFFR_CNT       ,0)) AS ACMLTMN_OFFR_CNT         /* 적립금제공건수         */
     , SUM(NVL(T5.DLR_ACMLTMN_OFFR_AMT   ,0)) AS DLR_ACMLTMN_OFFR_AMT     /* 달러적립금제공금액     */
     , SUM(NVL(T5.WON_ACMLTMN_OFFR_AMT   ,0)) AS WON_ACMLTMN_OFFR_AMT     /* 원화적립금제공금액     */
     , SUM(NVL(T4.ACMLTMN_USE_CNT        ,0)) AS ACMLTMN_USE_CNT          /* 적립금사용건수         */
     , SUM(NVL(T4.DLR_ACMLTMN_USE_AMT    ,0)) AS DLR_ACMLTMN_PYF_AMT      /* 달러적립금결제금액     */
     , SUM(NVL(T4.WON_ACMLTMN_USE_AMT    ,0)) AS WON_ACMLTMN_PYF_AMT      /* 원화적립금결제금액     */
     , SUM(NVL(T4.ACMLTMN_OURCOM_BDN_AMT ,0)) AS ACMLTMN_OURCOM_BDN_AMT   /* 적립금당사부담금액     */
     , SUM(NVL(T4.ACMLTMN_ALYCO_BDN_AMT  ,0)) AS ACMLTMN_ALYCO_BDN_AMT    /* 적립금제휴사부담금액   */
     , SUM(NVL(T2.DLR_ACMLTMN_USE_NSALAMT,0) + NVL(T2.DLR_ACMLTMN_ARSE_NSALAMT,0)) AS DLR_ACMLTMN_USE_NSALAMT  /* 달러적립금사용순매출액  */
     , SUM(NVL(T2.WON_ACMLTMN_USE_NSALAMT,0) + NVL(T2.WON_ACMLTMN_ARSE_NSALAMT,0)) AS WON_ACMLTMN_USE_NSALAMT  /* 원화적립금사용순매출액  */
     , SUM(NVL(T5.LDFP_ACMLT_CNT         ,0)) AS LDFP_ACMLT_CNT           /* LDFPAY적립건수         */
     , SUM(NVL(T5.DLR_LDFP_ACMLT_AMT     ,0)) AS DLR_LDFP_ACMLT_AMT       /* 달러LDFPAY적립금액     */
     , SUM(NVL(T5.WON_LDFP_ACMLT_AMT     ,0)) AS WON_LDFP_ACMLT_AMT       /* 원화LDFPAY적립금액     */
     , SUM(NVL(T5.LDFP_OURCOM_BDN_AMT    ,0)) AS LDFP_OURCOM_BDN_AMT      /* LDFPAY당사부담금액     */
     , SUM(NVL(T5.LDFP_ALYCO_BDN_AMT     ,0)) AS LDFP_ALYCO_BDN_AMT       /* LDFPAY제휴사부담금액   */
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
     , 0                                      AS DLR_CROS_EXCH_NSALAMT    /* 달러교차교환권순매출액 */
     , 0                                      AS WON_CROS_EXCH_NSALAMT    /* 원화교차교환권순매출액 */
  FROM
     (
        SELECT /*+ PARALLEL(4) */
               T1.PRMTNCD
             , T1.STD_DT
             , T1.INTG_MEMB_NO
             , T1.LANG_CD
             , T1.DVIC_CD
             , MAX(T1.ONLN_MEMB_NO        ) AS ONLN_MEMB_NO
             , MAX(T1.INTG_CUST_DISTING_NO) AS INTG_CUST_DISTING_NO
             , MAX(T1.CUST_DISTING_NO     ) AS CUST_DISTING_NO
             , MAX(T1.NATLT_CD            ) AS NATLT_CD
             , MAX(T1.ORD_MBSSYS_DVS_CD   ) AS ORD_MBSSYS_DVS_CD
             , MAX(T1.INTG_MEMB_GRD_CD    ) AS INTG_MEMB_GRD_CD
             , MAX(T1.IMGN_INFO_DVS_CD    ) AS IMGN_INFO_DVS_CD  /* 출입국정보구분코드 1:출국 */
             , MAX(T1.IMGN_DT             ) AS IMGN_DT           /* 출입국일자 */
          FROM FE_MK_PRMTN_ORD_SL T1  /* FE_MK_행사주문판매 */
           /*-- 변경적재 모수 ----------*/
         INNER JOIN FE_MK_ONLN_PRMTN_CHNG_TEMP T0
            ON T0.STD_DT       = T1.STD_DT
           AND T0.PRMTNCD      = T1.PRMTNCD
           AND T0.INTG_MEMB_NO = T1.INTG_MEMB_NO
         GROUP BY T1.PRMTNCD
                , T1.STD_DT
                , T1.INTG_MEMB_NO
                , T1.LANG_CD
                , T1.DVIC_CD
     ) T1
  LEFT OUTER JOIN (
     /* 주문관련 */
     SELECT /*+  PARALLEL(4) */
            T1.PRMTNCD
          , T1.STD_DT
          , T1.INTG_MEMB_NO
          , T1.LANG_CD
          , T1.DVIC_CD
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T2.DLR_PDCST ELSE 0 END)       AS DLR_PDCST_SUM_AMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T2.WON_PDCST ELSE 0 END)       AS WON_PDCST_SUM_AMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.DLR_TOT_NSALAMT ELSE 0 END) AS DLR_NSALAMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' THEN T1.WON_TOT_NSALAMT ELSE 0 END) AS WON_NSALAMT
          , SUM(T1.DLR_TOT_NSALAMT)                                                       AS DLR_PRMTN_NSALAMT
          , SUM(T1.WON_TOT_NSALAMT)                                                       AS WON_PRMTN_NSALAMT
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' THEN T1.DLR_TOT_NSALAMT ELSE 0 END)        AS OMNI_DLR_NSALAMT       /* 옴니달러순매출액 */
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' THEN T1.WON_TOT_NSALAMT ELSE 0 END)        AS OMNI_WON_NSALAMT       /* 옴니원화순매출액 */
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' AND T1.EXCH_PRMTN_APLY_YN ='Y' THEN T1.DLR_TOT_NSALAMT ELSE 0 END)       AS OMNI_DLR_EXCH_NSALAMT    /* 옴니달러교환권순매출액 */
          , SUM(CASE WHEN T1.OMNI_ORD_YN = 'Y' AND T1.EXCH_PRMTN_APLY_YN ='Y' THEN T1.WON_TOT_NSALAMT ELSE 0 END)       AS OMNI_WON_EXCH_NSALAMT    /* 옴니원화교환권순매출액 */
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' AND T1.PRMTN_LGCSF_CD = '007' THEN T1.DLR_TOT_NSALAMT ELSE 0 END) AS DLR_ACMLTMN_USE_NSALAMT
          , SUM(CASE WHEN T1.EXCH_PRMTN_APLY_YN = 'Y' AND T1.PRMTN_LGCSF_CD = '007' THEN T1.WON_TOT_NSALAMT ELSE 0 END) AS WON_ACMLTMN_USE_NSALAMT
          , SUM(T1.DLR_ACMLTMN_ARSE_NSALAMT)                                              AS DLR_ACMLTMN_ARSE_NSALAMT
          , SUM(T1.WON_ACMLTMN_ARSE_NSALAMT)                                              AS WON_ACMLTMN_ARSE_NSALAMT
          , SUM(T1.DLR_LDFP_ARSE_NSALAMT   )                                              AS DLR_LDFP_ARSE_NSALAMT    /* 달러LDFPAY유발순매출액 */
          , SUM(T1.WON_LDFP_ARSE_NSALAMT   )                                              AS WON_LDFP_ARSE_NSALAMT    /* 원화LDFPAY유발순매출액 */
          , SUM(T1.DLR_CROS_NSALAMT        )                                              AS DLR_CROS_NSALAMT         /* 달러교차순매출액       */
          , SUM(T1.WON_CROS_NSALAMT        )                                              AS WON_CROS_NSALAMT         /* 원화교차순매출액       */
       FROM FE_MK_PRMTN_ORD_SL T1 /* FE_MK_행사주문판매 */
        /*-- 변경적재 모수 ------------------*/
      INNER JOIN FE_MK_ONLN_PRMTN_CHNG_TEMP T0
         ON T0.STD_DT       = T1.STD_DT
        AND T0.PRMTNCD      = T1.PRMTNCD
        AND T0.INTG_MEMB_NO = T1.INTG_MEMB_NO
        ---------------------------------------
       LEFT OUTER JOIN
          (
            SELECT /*+ PARALLEL(4) */
                   T1.STD_DT
                 , T1.ONLN_ORD_NO
                 , T1.PRMTNCD
                 , T3.DLR_PDCST
                 , T3.WON_PDCST
              FROM FE_MK_PRMTN_ORD_SL  T1         /* FE_MK_행사주문판매 */
             INNER JOIN (
                   SELECT /*+ PARALLEL(4) */
                          T2.ONLN_ORD_NO
                        , T2.STD_DT
                        , SUM(T4.DLR_PDCST*T2.SALES_SIGN*T2.ORD_QTY) AS DLR_PDCST
                        , SUM(T4.WON_PDCST*T2.SALES_SIGN*T2.ORD_QTY) AS WON_PDCST
                     FROM (SELECT DISTINCT ONLN_ORD_NO FROM FE_MK_ORD_TEMP ) T1   /* 변경주문번호 */
                    INNER JOIN FE_SL_ORD_PROD T2  /* FE_SL_주문상품 */
                       ON T1.ONLN_ORD_NO = T2.ONLN_ORD_NO
                    INNER JOIN WL_IO_DD_PRICE T4  /* WL_일수불가격 */
                       ON T2.STD_DT       = T4.RCPDSBS_DT
                      AND T2.PRDCD        = T4.PRDCD
                      AND T2.STR_CD       = T4.STR_CD
                    GROUP BY T2.ONLN_ORD_NO
                        , T2.STD_DT
                 )  T3
                ON T1.ONLN_ORD_NO  = T3.ONLN_ORD_NO
               AND T1.STD_DT       = T3.STD_DT
             WHERE T1.DLR_TOT_NSALAMT > 0
          ) T2
         ON T1.ONLN_ORD_NO = T2.ONLN_ORD_NO
        AND T1.STD_DT      = T2.STD_DT
        AND T1.PRMTNCD     = T2.PRMTNCD
      GROUP BY T1.PRMTNCD
             , T1.STD_DT
             , T1.INTG_MEMB_NO
             , T1.LANG_CD
             , T1.DVIC_CD
     ) T2
   ON T1.PRMTNCD      = T2.PRMTNCD
  AND T1.STD_DT       = T2.STD_DT
  AND T1.INTG_MEMB_NO = T2.INTG_MEMB_NO
  AND T1.LANG_CD      = T2.LANG_CD
  AND T1.DVIC_CD      = T2.DVIC_CD
  LEFT OUTER JOIN
     ( /* 할인금액 */
        SELECT /*+ PARALLEL(4) */
              T1.PRMTNCD
            , T1.STD_DT
            , T1.INTG_MEMB_NO
            , T1.LANG_CD
            , T1.DVIC_CD
            , SUM(NVL(T4.OURCOM_BDNRT,100)/100.0*T3.WON_DC_AMT)          AS DC_OURCOM_BDN_AMT
            , SUM((100.0-NVL(T4.OURCOM_BDNRT,100))/100.0*T3.WON_DC_AMT)  AS DC_ALYCO_BDN_AMT
            , SUM(T3.DLR_DC_AMT)                                         AS DLR_PRMTN_DC_AMT   /* 달러할인금액 */
            , SUM(T3.WON_DC_AMT)                                         AS WON_PRMTN_DC_AMT   /* 원화할인금액 */
            , SUM(T3.DC_CNT)                                             AS DC_CNT             /* 할인건수     */
         FROM FE_MK_PRMTN_ORD_SL T1 /* FE_MK_행사주문판매 */
         /*-- 변경적재 모수 --------------------*/
        INNER JOIN FE_MK_ONLN_PRMTN_CHNG_TEMP T0
           ON T0.STD_DT       = T1.STD_DT
          AND T0.PRMTNCD      = T1.PRMTNCD
          AND T0.INTG_MEMB_NO = T1.INTG_MEMB_NO
          ---------------------------------------
        INNER JOIN (
              SELECT /*+ PARALLEL(4) */
                    T2.ORD_DT     AS STD_DT
                  , NVL(T4.PRMTNCD, CAST(T3.EVT_NO AS VARCHAR(10))) AS PRMTNCD
                  , T1.ORD_NO              AS ONLN_ORD_NO
                  , NVL(T6.INTG_MEMB_NO, T2.MBR_NO)    AS INTG_MEMB_NO
                  , SUM(NVL(T1.LCL_DSCNT_AMT,0)*DECODE(T1.ORD_DSCNT_STAT_CD,'01',1,'02',-1,0))  AS WON_DC_AMT
                  , SUM(NVL(T1.GLBL_DSCNT_AMT,0)*DECODE(T1.ORD_DSCNT_STAT_CD,'01',1,'02',-1,0)) AS DLR_DC_AMT
                  , SUM(DECODE(T1.ORD_DSCNT_STAT_CD,'01',1,'02',-1,0)) AS DC_CNT 
               FROM WE_OD_ORD_DSCNT  T1   /* WE_주문할인 */
              INNER JOIN WE_OD_ORD   T2   /* WE_주문 */
                 ON T1.ORD_NO            = T2.ORD_NO
                AND T1.ORD_DSCNT_KND_CD IN ('01','03') /* 01 세일 02 쿠폰 03 할인 */
              INNER JOIN (  --변경적재를 위한 temp
                    SELECT ONLN_ORD_NO
                         , STD_DT
                      FROM FE_MK_PRMTN_ORD_TEMP  
                     GROUP BY ONLN_ORD_NO
                         , STD_DT
                    ) T0
                 ON T1.ORD_NO = T0.ONLN_ORD_NO 
                AND T2.ORD_DT = T0.STD_DT
               LEFT OUTER JOIN WE_MT_EVT_FVR T3  /* WE_행사혜택 */
                 ON T1.ORD_DSCNT_REF_NO  = T3.EVT_FVR_NO
               LEFT OUTER JOIN ( /* 딜행사 */
                   SELECT DISTINCT T1.DEAL_EVT_FVR_NO
                        , T2.DEAL_EVT_NO
                        , CAST(T2.EVT_NO AS VARCHAR(10)) AS PRMTNCD
                     FROM WE_MT_DEAL_EVT_FVR  T1  /* WE_딜행사혜택 */
                    INNER JOIN WE_MT_DEAL_EVT T2  /* WE_딜행사 */
                       ON T1.DEAL_EVT_NO =  T2.DEAL_EVT_NO
                      AND T2.EVT_NO IS NOT NULL
                   ) T4    
                 ON T1.ORD_DSCNT_REF_NO  = T4.DEAL_EVT_FVR_NO
               LEFT OUTER JOIN DE_ONLN_MEMB T6  /* DE_온라인회원 */
                 ON T2.MBR_NO            = T6.ONLN_MEMB_NO
              WHERE T1.ORD_DSCNT_STAT_CD IN ('01','02')     
              GROUP BY  T2.ORD_DT
                  , NVL(T4.PRMTNCD, CAST(T3.EVT_NO AS VARCHAR(10)))
                  , T1.ORD_NO  
                  , NVL(T6.INTG_MEMB_NO, T2.MBR_NO)    
            ) T3 
           ON T1.STD_DT         = T3.STD_DT
          AND T1.ONLN_ORD_NO    = T3.ONLN_ORD_NO
          AND T1.PRMTNCD        = T3.PRMTNCD
          AND T1.INTG_MEMB_NO   = T3.INTG_MEMB_NO
        INNER JOIN D_PRMTN T4
           ON T1.PRMTNCD        = T4.PRMTNCD
        WHERE T1.PRMTN_LGCSF_CD IN ('001','014')  /* 할인 딜 */
        GROUP BY T1.PRMTNCD
               , T1.STD_DT
               , T1.INTG_MEMB_NO
               , T1.LANG_CD
               , T1.DVIC_CD 
     ) T3
   ON T1.PRMTNCD      = T3.PRMTNCD
  AND T1.STD_DT       = T3.STD_DT
  AND T1.INTG_MEMB_NO = T3.INTG_MEMB_NO
  AND T1.LANG_CD      = T3.LANG_CD
  AND T1.DVIC_CD      = T3.DVIC_CD
 LEFT OUTER JOIN (
      /* LDFPAY적립, 적립금적립은 제외 */
      SELECT /*+  PARALLEL(4) */
            T1.STD_DT
          , T1.PRMTNCD
          , T1.INTG_MEMB_NO
          , T1.LANG_CD
          , MAX(T1.DVIC_CD)  AS DVIC_CD
          , 0       AS ACMLTMN_OFFR_CNT     -- SUM(CASE WHEN T1.PRMTN_LGCSF_CD = '007' THEN T1.OFFER_PRESTAT_CNT ELSE 0 END)     AS ACMLTMN_OFFR_CNT
          , 0       AS DLR_ACMLTMN_OFFR_AMT -- SUM(CASE WHEN T1.PRMTN_LGCSF_CD = '007' THEN T1.DLR_OFFER_PRESTAT_AMT ELSE 0 END) AS DLR_ACMLTMN_OFFR_AMT
          , 0       AS WON_ACMLTMN_OFFR_AMT -- SUM(CASE WHEN T1.PRMTN_LGCSF_CD = '007' THEN T1.WON_OFFER_PRESTAT_AMT ELSE 0 END) AS WON_ACMLTMN_OFFR_AMT
          , SUM(T1.OFFER_PRESTAT_CNT    )        AS LDFP_ACMLT_CNT
          , SUM(T1.DLR_OFFER_PRESTAT_AMT)        AS DLR_LDFP_ACMLT_AMT
          , SUM(T1.WON_OFFER_PRESTAT_AMT)        AS WON_LDFP_ACMLT_AMT
          , SUM(T1.OFFER_PRESTAT_OURCOM_BDN_AMT) AS LDFP_OURCOM_BDN_AMT
          , SUM(T1.WON_OFFER_PRESTAT_AMT-T1.OFFER_PRESTAT_OURCOM_BDN_AMT) AS LDFP_ALYCO_BDN_AMT
       FROM FE_MK_BNFT_OFFER_DTL T1  /* FE_MK_혜택OFFER상세 */
        /*-- 변경적재 모수 --------------------*/
      INNER JOIN FE_MK_ONLN_PRMTN_CHNG_TEMP T0
         ON T0.STD_DT             = T1.STD_DT
        AND T0.PRMTNCD            = T1.PRMTNCD
        AND T0.INTG_MEMB_NO       = T1.INTG_MEMB_NO
        AND T1.STD_DT       BETWEEN T0.PRMTN_STRT_DT AND T0.PRMTN_END_DT
      WHERE T1.ONLN_OFFER_CLSF_CD  = '07'       /* 07:LDFPAY */
        AND T1.OFFER_PRESTAT_CNT <> 0
      GROUP BY T1.STD_DT
             , T1.PRMTNCD
             , T1.INTG_MEMB_NO
             , T1.LANG_CD
        --     , T1.DVIC_CD
     ) T5
    ON T1.PRMTNCD      = T5.PRMTNCD
   AND T1.STD_DT       = T5.STD_DT
   AND T1.INTG_MEMB_NO = T5.INTG_MEMB_NO
   AND T1.LANG_CD      = T5.LANG_CD
  --  AND T1.DVIC_CD      = T5.DVIC_CD
  LEFT OUTER JOIN (
       /* 적립금사용 */
      SELECT /*+  PARALLEL(4) */
            T1.STD_DT
          , T1.PRMTNCD
          , T1.INTG_MEMB_NO  INTG_MEMB_NO
          , T1.LANG_CD
          , T1.DVIC_CD
          , SUM(T1.OFFER_USE_CNT    )        AS ACMLTMN_USE_CNT
          , SUM(T1.DLR_OFFER_USE_AMT)        AS DLR_ACMLTMN_USE_AMT
          , SUM(T1.WON_OFFER_USE_AMT)        AS WON_ACMLTMN_USE_AMT
          , SUM(T1.OFFER_USE_OURCOM_BDN_AMT) AS ACMLTMN_OURCOM_BDN_AMT
          , SUM(T1.WON_OFFER_USE_AMT)-SUM(T1.OFFER_USE_OURCOM_BDN_AMT) AS ACMLTMN_ALYCO_BDN_AMT
       FROM FE_MK_BNFT_OFFER_DTL T1  /* FE_MK_혜택OFFER상세 */
        /*-- 변경적재 모수 --------------------*/
      INNER JOIN FE_MK_ONLN_PRMTN_CHNG_TEMP T0
         ON T0.STD_DT             = T1.STD_DT
        AND T0.PRMTNCD            = T1.PRMTNCD
        AND T0.INTG_MEMB_NO       = T1.INTG_MEMB_NO
        AND T1.STD_DT       BETWEEN T0.PRMTN_STRT_DT AND T0.PRMTN_END_DT
      WHERE T1.ONLN_OFFER_CLSF_CD ='02'      /* 02:적립금 */
        AND T1.PRMTN_LGCSF_CD NOT IN ('001','005')       /* 하나의 행사가 적립금, LDFPAY의 오퍼를 갖는경우 중복제외를 위해 사용 */
        AND T1.OFFER_USE_CNT <> 0
      GROUP BY T1.STD_DT
             , T1.PRMTNCD
             , T1.INTG_MEMB_NO
             , T1.LANG_CD
             , T1.DVIC_CD
      ) T4
    ON T1.PRMTNCD      = T4.PRMTNCD
   AND T1.STD_DT       = T4.STD_DT
   AND T1.INTG_MEMB_NO = T4.INTG_MEMB_NO
   AND T1.LANG_CD      = T4.LANG_CD
   AND T1.DVIC_CD      = T4.DVIC_CD
 INNER JOIN (
       SELECT T1.PRMTNCD
            , T1.PRMTN_LGCSF_CD
            , T1.PRMTN_MDCSF_CD
            , T1.MDPRCD
            , T1.LGPRCD
         FROM D_PRMTN t1
        WHERE NOT EXISTS (SELECT 1 FROM D_LGPROMO T2 WHERE T1.LGPRCD = T2.LGPRCD AND T2.PRMTN_ACTRSLT_OBJ_XCLUD_YN = 'Y')
     ) T9
    ON T1.PRMTNCD = T9.PRMTNCD
/* INNER JOIN D_PRMTN T9                     */
/*    ON T1.PRMTNCD = T9.PRMTNCD             */
/* INNER JOIN D_LGPROMO T7                   */
/*    ON T9.LGPRCD =  T7.LGPRCD              */
/*   AND T7.PRMTN_ACTRSLT_OBJ_XCLUD_YN = 'N' */ /* 행사실적대상제외여부 */
 GROUP BY T1.STD_DT
        , T1.PRMTNCD
        , T1.INTG_MEMB_NO
        , NVL(T1.LANG_CD ,'z')
        , NVL(T1.DVIC_CD ,'z')
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_MERGE_01  *****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

MERGE /*+ PARALLEL(4) */
 INTO FE_MK_PRMTN_ACTRSLT T1
USING ( SELECT T2.PRMTNCD
             , T2.STD_DT
             , T2.INTG_MEMB_NO
             , T2.LANG_CD
             , T2.DVIC_CD
             , T2.ONLN_MEMB_NO
             , T2.DLR_NSALAMT
             , T2.WON_NSALAMT
          FROM FE_MK_PRMTN_ACTRSLT T2
         INNER JOIN
             ( SELECT DISTINCT
                      T2.PRMTNCD
                    --, T3.LANG_CD
                    --, T3.DVIC_CD
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
                   ON T1.CAMP_ID       = T3.PRMTNCD
                  AND T1.ONLN_MEMB_NO  = T3.CHNG_ONLN_MEMB_NO
                WHERE NVL(TRIM(T1.SEND_MEMB_CD),T2.CHNL_TYPE_CD) <> T2.CHNL_TYPE_CD
             ) T3
            ON T2.PRMTNCD      = T3.PRMTNCD
           AND T2.ONLN_MEMB_NO = T3.ONLN_MEMB_NO
       ) T2
    ON (T1.PRMTNCD       = T2.PRMTNCD
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