--1. 행사별
 SELECT M.LGPRCD                              AS LGPRCD                    /* 대행사코드 */
         , M.LGPROMO_NM                          AS LGPROMO_NM                /* 대행사명 */
         , M.MDPRCD                              AS MDPRCD                    /* 중행사코드 */
         , M.MIDPROMO_NM                         AS MIDPROMO_NM               /* 중행사명 */
         , M.PRMTNCD                             AS PRMTNCD                   /* 소행사코드 */
         , M.PRMTN_NM                            AS PRMTN_NM                  /* 소행사명 */
       , CASE WHEN M.CHNL_TYPE_CD = '1' THEN (SELECT REGEXP_REPLACE(LISTAGG(B1.STR_NM, '/' ON OVERFLOW TRUNCATE) WITHIN GROUP(ORDER BY B1.PRMTNCD), '([^/]+)(/\1)*(/|$)', '\1\3' ) AS STR_LANG_NM
                                                  FROM (SELECT B1.PRMTNCD 
                                                             , CD2.STR_NM
                                                          FROM LDF_DW.D_PRMTN B1                     /* D_행사 */
                                           , LDF_DW.WL_LC_PRMTN_OFFR_CNDT B2       /* LC_행사제공조건 테이블 */
                                                             , LDF_DW.D_STR CD2                      /* 지점 마스터 테이블 */
                                                         WHERE 1                       = 1
                                                           AND B1.LGPRCD               = M.LGPRCD
                                                           AND B1.MDPRCD               = M.MDPRCD 
                                                           AND B2.PRMTNCD              = M.PRMTNCD
                                                           AND B1.PRMTNCD              = B2.PRMTNCD
                                                           AND B2.PRMTN_CNDT_VAL       = CD2.STR_CD
                                                           AND B1.CHNL_TYPE_CD         = '02'          /* 채널 - 오프라인 */
                                                           AND B2.PRMTN_OFFR_CNDT_CD   = '01'          /* 지점구분코드 */
                                                           AND CD2.STR_CD              <> '907'        /* 지점 인천공항B 제외 */  
                                                         GROUP BY B1.PRMTNCD 
                                                             , CD2.STR_NM
                                                        ) B1
                                                 ) 
                                          ELSE (SELECT REGEXP_REPLACE(LISTAGG(CD2.CD_DESC, '/' ON OVERFLOW TRUNCATE) WITHIN GROUP(ORDER BY B1.PRMTNCD), '([^/]+)(/\1)*(/|$)', '\1\3') AS STR_LANG_NM
                                                  FROM LDF_DW.D_PRMTN B1                        /* D_행사 */
                                         , LDF_DW.WL_LC_PRMTN_OFFR_CNDT B2          /* LC_행사제공조건 테이블 */
                                                     , LDF_DW.WE_ST_COM_CD_DTL CD2              /* 공통테이블 */
                                                 WHERE 1                       = 1
                                                   AND B1.LGPRCD               = M.LGPRCD
                                                   AND B1.MDPRCD               = M.MDPRCD 
                                                   AND B1.PRMTNCD              = M.PRMTNCD
                                                   AND B1.PRMTNCD              = B2.PRMTNCD
                                                   AND B2.PRMTN_CNDT_VAL       = CD2.COM_CD
                                                   AND B1.CHNL_TYPE_CD         = '01'           /* 채널 - 온라인 */
                                                   AND CD2.COM_GRP_CD          = 'LANG_CD'
                                                   AND B2.PRMTN_OFFR_CNDT_CD   = '01'           /* 구분코드 */                                                                        
                                               ) 
           END                                    AS STR_LANG_NM               /* 지점/언어 */
         , M.PRMTN_LGCSF_CD                       AS PRMTN_LGCSF_CD            /* 행사대분류코드 */
         , M.PRMTN_LGCSF_NM                       AS PRMTN_LGCSF_NM            /* 행사대분류명 */
         , M.PRMTN_MDCSF_CD                       AS PRMTN_MDCSF_CD            /* 행사중분류코드 */
         , M.PRMTN_MDCSF_NM                       AS PRMTN_MDCSF_NM            /* 행사대분류명 */
         , M.PRMTN_STRT_DT                        AS PRMTN_STRT_DT             /* 행사시작일자 */
         , M.PRMTN_END_DT                         AS PRMTN_END_DT              /* 행사종료일자 */
         , NVL(M.WON_EXCH_NSALAMT,0)              AS WON_EXCH_NSALAMT          /* 교환권매출_실적(원화) */
         , NVL(M.DLR_EXCH_NSALAMT,0)              AS DLR_EXCH_NSALAMT          /* 교환권매출_실적(달러) */
         , NVL(M.EXCH_SALES_CNT,0)                AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
         , NVL(M.WON_ARSE_NSALAMT_CUSTRN,0)       AS WON_ARSE_NSALAMT_CUSTRN   /* 교환권매출_객단가(원화) */
         , NVL(M.DLR_ARSE_NSALAMT_CUSTRN,0)       AS DLR_ARSE_NSALAMT_CUSTRN   /* 교환권매출_객단가(달러) */
         , NVL(M.WON_PRMTN_NSALAMT,0)             AS WON_PRMTN_NSALAMT         /* 행사매출_실적(원화) */
         , NVL(M.DLR_PRMTN_NSALAMT,0)             AS DLR_PRMTN_NSALAMT         /* 행사매출_실적(달러) */
         , NVL(M.PRMTN_NSALAMT_CNT,0)             AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
         , NVL(M.WON_PRMTN_NSALAMT_CUSTRN,0)      AS WON_PRMTN_NSALAMT_CUSTRN  /* 행사매출_객단가(원화) */
         , NVL(M.DLR_PRMTN_NSALAMT_CUSTRN,0)      AS DLR_PRMTN_NSALAMT_CUSTRN  /* 행사매출_객단가(달러) */
         , NVL(M.WON_LDFP_ACMLT_AMT,0)            AS WON_LDFP_ACMLT_AMT        /* LDFPAY_증정금액(원화) */
         , NVL(M.LDFP_ACMLT_CNT,0)                AS LDFP_ACMLT_CNT            /* LDFPAY_증정건수 */
         , NVL(M.LDFP_ACMLT_CUST_NBR,0)           AS LDFP_ACMLT_CUST_NBR       /* LDFPAY_증정객수 */
         , NVL(M.FREELDFP_USE_AMT,0)              AS FREELDFP_USE_AMT          /* FREE_LDFPAY_사용금액 */
         , NVL(M.FREELDFP_USE_CNT,0)              AS FREELDFP_USE_CNT          /* FREE_LDFPAY_사용건수 */
         , NVL(M.FREELDFP_USE_CUST_NBR,0)         AS FREELDFP_USE_CUST_NBR     /* FREE_LDFPAY_사용객수 */
         , NVL(M.FREELDFP_ACMLT_AMT,0)            AS FREELDFP_ACMLT_AMT          /* FREE_LDFPAY_증정금액 */
         , NVL(M.FREELDFP_ACMLT_CNT,0)            AS FREELDFP_ACMLT_CNT          /* FREE_LDFPAY_증정건수 */
         , NVL(M.FREELDFP_ACLMLT_CUST_NBR,0)      AS FREELDFP_ACLMLT_CUST_NBR     /* FREE_LDFPAY_증정객수 */
         , NVL(M.DC_AMT,0)                        AS DC_AMT                    /* 할인_금액 */
         , NVL(M.DC_CNT,0)                        AS DC_CNT                    /* 할인_건수 */
         , NVL(M.BDN_CUST_NBR,0)                  AS BDN_CUST_NBR              /* 할인_객수 */
         , NVL(M.FGF_PRESTAT_AMT,0)               AS FGF_PRESTAT_AMT           /* 사은품증정_금액 */
         , NVL(M.FGF_PRESTAT_CNT,0)               AS FGF_PRESTAT_CNT           /* 사은품증정_건수 */
         , NVL(M.FGF_PRESTAT_CUST_NBR,0)          AS FGF_PRESTAT_CUST_NBR      /* 사은품증정_객수 */
         , NVL(M.ACMLTMN_OFFR_CNT,0)              AS ACMLTMN_OFFR_CNT          /* 적립금증정_건수 */
         , NVL(M.DLR_ACMLTMN_OFFR_AMT,0)          AS DLR_ACMLTMN_OFFR_AMT      /* 적립금증정_금액(원화) */
         , NVL(M.ACMLTMN_OFFR_CUST_NBR,0)         AS ACMLTMN_OFFR_CUST_NBR     /* 적립금증정_객수 */
         , NVL(M.ACMLTMN_USE_CNT,0)               AS ACMLTMN_USE_CNT           /* 적립금사용_건수 */
         , NVL(M.WON_ACMLTMN_PYF_AMT,0)           AS WON_ACMLTMN_PYF_AMT       /* 적립금사용_금액(원화) */
         , NVL(M.ACMLTMN_USE_CUST_NBR,0)          AS ACMLTMN_USE_CUST_NBR      /* 적립금사용_객수 */
         , NVL(M.TOT_PRESTAT_AMT,0)               AS TOT_PRESTAT_AMT           /* 지원금액(전체) */
         , NVL(M.PRESTAT_OURCOM_BDN_AMT,0)        AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사) */
         , NVL(M.PRESTAT_ALYCO_BDN_AMT,0)         AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사) */
         , NVL(M.DC_TOT_PRESTAT_RATE,0)           AS OURCOM_PRESTAT_RATE       /* 지원율(전체) */   
         , NVL(M.OURCOM_PRESTAT_RATE,0)           AS DC_TOT_PRESTAT_RATE       /* 지원율(당사) */     
         , NVL(M.TOT_BDN_AMT,0)                   AS TOT_BDN_AMT               /* 할인금액(전체) */
         , NVL(M.DC_OURCOM_BDN_AMT,0)             AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
         , NVL(M.DC_ALYCO_BDN_AMT,0)              AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */
         , NVL(M.TOT_DC_CNT,0)                    AS TOT_DC_CNT                /* 할인건수 */
         , NVL(M.WON_TOT_DC_AMT,0)                AS WON_TOT_DC_AMT            /* 원화총할인금액 */
         , NVL(M.DC_TOT_BDN_RATE,0)               AS DC_OURCOM_BDN_RATE           /* 할인율(전체) */
         , NVL(M.DC_OURCOM_BDN_RATE,0)            AS DC_TOT_BDN_RATE        /* 할인율(당사) */  
         , NVL(M.TOT_PRESTAT_AMT+M.TOT_BDN_AMT,0) AS PRMTN_EXP                 /* 행사비용(증정금액+할인금액) */
         , NVL(ROUND(((M.TOT_PRESTAT_AMT + M.TOT_BDN_AMT) / DECODE(M.WON_PRMTN_NSALAMT, 0, NULL, M.WON_PRMTN_NSALAMT))*100,1),0) AS INTG_SUPTRT   /* 통합지원율(증정금액+할인금액) / 원화행사순매출액 */ 
         , NVL(M.WON_ARSE_NSALAMT,0)              AS WON_ARSE_NSALAMT          /* 증정유발매출(원화) */
         , NVL(M.ONLN_WON_ARSE_NSALAMT,0)         AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출(원화) */
         , NVL(M.ONLN_DLR_ARSE_NSALAMT,0)         AS ONLN_DLR_ARSE_NSALAMT     /* 온라인유발매출(달러) */
         , NVL(M.OFLN_WON_ARSE_NSALAMT,0)         AS OFLN_WON_ARSE_NSALAMT     /* 오프라인유발매출(원화) */
         , NVL(M.OFLN_DLR_ARSE_NSALAMT,0)         AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출(달러) */
         , NVL(M.OMNI_WON_EXCH_NSALAMT,0)         AS OMNI_WON_EXCH_NSALAMT     /* 교환권옴니매출(원화) */
         , NVL(M.OMNI_DLR_EXCH_NSALAMT,0)         AS OMNI_DLR_EXCH_NSALAMT     /* 교환권옴니매출(달러) */
         , NVL(M.OMNI_WON_NSALAMT,0)              AS OMNI_WON_NSALAMT          /* 행사옴니매출(원화) */
         , NVL(M.OMNI_DLR_NSALAMT,0)              AS OMNI_DLR_NSALAMT          /* 행사옴니매출(달러) */
         , M.RGSTPSN_ID                           AS RGSTPSN_ID                /* 등록자ID */
         , M.USR_NM                               AS RGSTPSN_NM                /* 등록자명 */
         , M.RGST_DEPTCD                          AS RGST_DEPTCD               /* 등록부서코드 */
         , M.DEPT_NM                              AS RGST_DEPT_NM              /* 등록부서명 */
      FROM (SELECT T.LGPRCD                                                                                                                                                                                    AS LGPRCD
                  , T.LGPROMO_NM                                                                                                                                                                               AS LGPROMO_NM
                  , T.MDPRCD                                                                                                                                                                                   AS MDPRCD
                  , T.MIDPROMO_NM                                                                                                                                                                              AS MIDPROMO_NM
                  , T.PRMTNCD                                                                                                                                                                                  AS PRMTNCD
                  , T.PRMTN_NM                                                                                                                                                                                 AS PRMTN_NM
                  , MAX(T.CHNL_TYPE_CD)                                                                                                                                                                        AS CHNL_TYPE_CD                  /* 지점/언어 구분 명(언어) */
                  , MAX(T.PRMTN_LGCSF_CD)                                                                                                                                                                      AS PRMTN_LGCSF_CD
                  , MAX(T.PRMTN_LGCSF_NM)                                                                                                                                                                      AS PRMTN_LGCSF_NM
                  , MAX(T.PRMTN_MDCSF_CD)                                                                                                                                                                      AS PRMTN_MDCSF_CD
                  , MAX(T.PRMTN_MDCSF_NM)                                                                                                                                                                      AS PRMTN_MDCSF_NM
                  , MAX(T.PRMTN_STRT_DT)                                                                                                                                                                       AS PRMTN_STRT_DT
                  , MAX(T.PRMTN_END_DT)                                                                                                                                                                        AS PRMTN_END_DT
                  , SUM(T.WON_EXCH_NSALAMT)                                                                                                                                                                    AS WON_EXCH_NSALAMT
              , SUM(T.DLR_EXCH_NSALAMT)                                                                                                                                                                    AS DLR_EXCH_NSALAMT
              , COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0    THEN T.EXCH_SALES_CNT  END)                                                                                                 AS EXCH_SALES_CNT        
              , ROUND(NVL(SUM(T.WON_EXCH_NSALAMT)   
                / DECODE(COUNT(DISTINCT CASE WHEN T.WON_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.WON_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END)),0))        AS WON_ARSE_NSALAMT_CUSTRN
              , ROUND(NVL(SUM(T.DLR_EXCH_NSALAMT)   
                / DECODE(COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END)),0))        AS DLR_ARSE_NSALAMT_CUSTRN           
              , SUM(T.WON_PRMTN_NSALAMT)                                                                                                                                                                   AS WON_PRMTN_NSALAMT
              , SUM(T.DLR_PRMTN_NSALAMT)                                                                                                                                                                   AS DLR_PRMTN_NSALAMT
              , COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0   THEN T.PRMTN_NSALAMT_CNT END)                                                                                               AS PRMTN_NSALAMT_CNT
              , ROUND(NVL(SUM(T.WON_PRMTN_NSALAMT)  
                / DECODE(COUNT(DISTINCT CASE WHEN T.WON_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.WON_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END)),0))  AS WON_PRMTN_NSALAMT_CUSTRN
              , ROUND(NVL(SUM(T.DLR_PRMTN_NSALAMT)  
                / DECODE(COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END)),0))  AS DLR_PRMTN_NSALAMT_CUSTRN
              , SUM(T.WON_LDFP_ACMLT_AMT)                                                                                                                                                                  AS WON_LDFP_ACMLT_AMT
              , SUM(T.LDFP_ACMLT_CNT)                                                                                                                                                                      AS LDFP_ACMLT_CNT
              , COUNT(DISTINCT CASE WHEN T.WON_LDFP_ACMLT_AMT IS NOT NULL AND T.WON_LDFP_ACMLT_AMT <> 0  THEN T.INTG_MEMB_NO END)                                                                          AS LDFP_ACMLT_CUST_NBR
              , SUM(T.FREELDFP_USE_AMT)                                                                                                                                                                    AS FREELDFP_USE_AMT
              , SUM(T.FREELDFP_USE_CNT)                                                                                                                                                                    AS FREELDFP_USE_CNT
              , COUNT(DISTINCT CASE WHEN T.FREELDFP_USE_AMT   IS NOT NULL AND T.FREELDFP_USE_AMT <> 0    THEN T.INTG_MEMB_NO END)                                                                          AS FREELDFP_USE_CUST_NBR
                  , SUM(T.FREELDFP_ACMLT_AMT)                                                                                                                                                                  AS FREELDFP_ACMLT_AMT          /* 프리LDFPAY증정금액(프리LDFPAY적립금액) */
                  , SUM(T.FREELDFP_ACMLT_CNT)                                                                                                                                                                  AS FREELDFP_ACMLT_CNT          /* 프리LDFPAY증정건수(프리LDFPAY적립건수) */ 
              , COUNT(DISTINCT CASE WHEN T.FREELDFP_ACMLT_AMT IS NOT NULL AND T.FREELDFP_ACMLT_AMT <> 0  THEN T.INTG_MEMB_NO END)                                                                          AS FREELDFP_ACLMLT_CUST_NBR
              , SUM(T.WON_TOT_DC_AMT)                                                                                                                                                                      AS DC_AMT
              , SUM(T.DC_CNT)                                                                                                                                                                              AS DC_CNT
              , COUNT(DISTINCT CASE WHEN T.WON_TOT_DC_AMT     IS NOT NULL AND T.WON_TOT_DC_AMT <> 0      THEN T.INTG_MEMB_NO END)                                                                          AS BDN_CUST_NBR
              , SUM(T.FGF_PRESTAT_AMT)                                                                                                                                                                     AS FGF_PRESTAT_AMT
              , SUM(T.FGF_PRESTAT_CNT)                                                                                                                                                                     AS FGF_PRESTAT_CNT
              , COUNT(DISTINCT CASE WHEN T.FGF_PRESTAT_AMT    IS NOT NULL AND T.FGF_PRESTAT_AMT <> 0     THEN T.INTG_MEMB_NO END)                                                                          AS FGF_PRESTAT_CUST_NBR
                  , MAX(T.ACMLTMN_OFFR_CUST)                                                                                                                                                                   AS ACMLTMN_OFFR_CUST_NBR     /* 적립금제공건수 */
                  , MAX(T.ACMLTMN_OFFR_CNT)                                                                                                                                                                    AS ACMLTMN_OFFR_CNT          /* 적립금증정_건수 */
                  , MAX(T.DLR_ACMLTMN_OFFR_AMT)                                                                                                                                                                AS DLR_ACMLTMN_OFFR_AMT      /* 적립금증정_건수 */ 
              , SUM(T.ACMLTMN_USE_CNT)                                                                                                                                                                     AS ACMLTMN_USE_CNT
              , SUM(T.WON_ACMLTMN_PYF_AMT)                                                                                                                                                                 AS WON_ACMLTMN_PYF_AMT
              , COUNT(DISTINCT CASE WHEN T.WON_ACMLTMN_PYF_AMT IS NOT NULL AND T.WON_ACMLTMN_PYF_AMT <> 0 THEN T.INTG_MEMB_NO END)                                                                         AS ACMLTMN_USE_CUST_NBR
              , SUM(T.TOT_PRESTAT_AMT)                                                                                                                                                                     AS TOT_PRESTAT_AMT
              , SUM(T.PRESTAT_OURCOM_BDN_AMT)                                                                                                                                                              AS PRESTAT_OURCOM_BDN_AMT
              , SUM(T.PRESTAT_ALYCO_BDN_AMT)                                                                                                                                                               AS PRESTAT_ALYCO_BDN_AMT
                  , ROUND(NVL(SUM(T.TOT_PRESTAT_AMT) / DECODE(SUM(T.WON_EXCH_NSALAMT),0,NULL,SUM(T.WON_EXCH_NSALAMT)),0) * 100,1)                                                                               AS DC_TOT_PRESTAT_RATE /* 지원율(전체) - ADD */
                  , ROUND(NVL(SUM(T.PRESTAT_OURCOM_BDN_AMT) / DECODE(SUM(T.WON_EXCH_NSALAMT),0,NULL,SUM(T.WON_EXCH_NSALAMT)),0) * 100,1)                                                                       AS OURCOM_PRESTAT_RATE /* 지원율(당사) */
              , SUM(T.WON_TOT_DC_AMT)                                                                                                                                                                      AS TOT_BDN_AMT
              , SUM(T.DC_OURCOM_BDN_AMT)                                                                                                                                                                   AS DC_OURCOM_BDN_AMT
              , SUM(T.DC_ALYCO_BDN_AMT)                                                                                                                                                                    AS DC_ALYCO_BDN_AMT
              , SUM(T.DC_CNT)                                                                                                                                                                              AS TOT_DC_CNT
              , SUM(T.WON_TOT_DC_AMT)                                                                                                                                                                      AS WON_TOT_DC_AMT
                  , ROUND(NVL(SUM(T.WON_TOT_DC_AMT)     / DECODE(SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT),0,NULL,SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT)),0) * 100,1)                           AS DC_TOT_BDN_RATE /* 할인율(전체) */
                  , ROUND(NVL(SUM(T.DC_OURCOM_BDN_AMT)     / DECODE(SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT),0,NULL,SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT)),0) * 100,1)                        AS DC_OURCOM_BDN_RATE /* 할인율(당사) */
              , SUM(T.WON_ARSE_NSALAMT)                                                                                                                                                                    AS WON_ARSE_NSALAMT
              , SUM(T.DLR_ARSE_NSALAMT)                                                                                                                                                                    AS DLR_ARSE_NSALAMT
              , SUM(T.ONLN_WON_ARSE_NSALAMT)                                                                                                                                                               AS ONLN_WON_ARSE_NSALAMT
              , SUM(T.ONLN_DLR_ARSE_NSALAMT)                                                                                                                                                               AS ONLN_DLR_ARSE_NSALAMT
              , SUM(T.OFLN_WON_ARSE_NSALAMT)                                                                                                                                                               AS OFLN_WON_ARSE_NSALAMT
              , SUM(T.OFLN_DLR_ARSE_NSALAMT)                                                                                                                                                               AS OFLN_DLR_ARSE_NSALAMT
              , SUM(T.OMNI_WON_EXCH_NSALAMT)                                                                                                                                                               AS OMNI_WON_EXCH_NSALAMT
              , SUM(T.OMNI_DLR_EXCH_NSALAMT)                                                                                                                                                               AS OMNI_DLR_EXCH_NSALAMT
              , SUM(T.OMNI_WON_NSALAMT)                                                                                                                                                                    AS OMNI_WON_NSALAMT
              , SUM(T.OMNI_DLR_NSALAMT)                                                                                                                                                                    AS OMNI_DLR_NSALAMT
              , MAX(T.RGSTPSN_ID)                                                                                                                                                                          AS RGSTPSN_ID
              , MAX(T.USR_NM)                                                                                                                                                                              AS USR_NM
              , MAX(T.RGST_DEPTCD)                                                                                                                                                                         AS RGST_DEPTCD
              , MAX(T.DEPT_NM)                                                                                                                                                                             AS DEPT_NM
               FROM (SELECT /* FE_MK_PRMTN_ACTRSLT FE_MK_행사실적(온라인) */ 
                            A6.LGPRCD
                          , A1.LGPROMO_NM 
                          , A6.MDPRCD
                          , A2.MIDPROMO_NM 
                          , A6.PRMTNCD
                          , A3.PRMTN_NM
                          , A3.ONOFF_DVS_CD                                                                                                     AS CHNL_TYPE_CD              /* 채널구분(1:오프라인,2:온라인) */
                          , A3.PRMTN_LGCSF_CD                                                                                                   AS PRMTN_LGCSF_CD            /* 행사대분류코드 */
                          , A4.PRMTN_LGCSF_NM                                                                                                   AS PRMTN_LGCSF_NM            /* 행사대분류코드 */                                              
                          , A3.PRMTN_MDCSF_CD                                                                                                   AS PRMTN_MDCSF_CD            /* 행사중분류코드 */ 
                          , A5.PRMTN_MDCSF_NM                                                                                                   AS PRMTN_MDCSF_NM            /* 행사중분류명 */ 
                          , A3.PRMTN_STRT_DT                                                                                                    AS PRMTN_STRT_DT             /* 행사시작일자 */
                          , A3.PRMTN_END_DT                                                                                                     AS PRMTN_END_DT              /* 행사종료일자 */
                          , A6.INTG_MEMB_NO                                                                                                     AS INTG_MEMB_NO              /* 통합회원번호 */
                          , A6.WON_NSALAMT                                                                                                      AS WON_EXCH_NSALAMT          /* 원화교환권순매출액(=원화순매출액) */
                          , A6.DLR_NSALAMT                                                                                                      AS DLR_EXCH_NSALAMT          /* 달러교환권순매출액(=달러순매출액) */
                          , (CASE WHEN A6.DLR_NSALAMT       IS NOT NULL THEN A6.INTG_MEMB_NO ELSE NULL END)                   AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
                          , A6.WON_PRMTN_NSALAMT                                                                                                AS WON_PRMTN_NSALAMT         /* 원화행사순매출액 */
                          , A6.DLR_PRMTN_NSALAMT                                                                                                AS DLR_PRMTN_NSALAMT         /* 달러행사순매출액 */
                          , (CASE WHEN A6.DLR_PRMTN_NSALAMT IS NOT NULL THEN A6.INTG_MEMB_NO ELSE NULL END)                   AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
                          , A6.WON_LDFP_ACMLT_AMT                                                                                               AS WON_LDFP_ACMLT_AMT        /* LDFPAY 적립금액(원화) */
                          , A6.LDFP_ACMLT_CNT                                                                                                   AS LDFP_ACMLT_CNT            /* LDFPAY 적립건수 */
                          , NULL                                                                                                                AS FREELDFP_USE_AMT          /* 프리LDFPAY사용금액 */
                          , NULL                                                                                                                AS FREELDFP_USE_CNT          /* 프리LDFPAY사용건수 */
                          , NULL                                                                                                                AS FREELDFP_ACMLT_AMT        /* 프리LDFPAY증정금액(프리LDFPAY적립금액) */
                          , NULL                                                                                                                AS FREELDFP_ACMLT_CNT        /* 프리LDFPAY증정건수(프리LDFPAY적립건수) */ 
                          , A6.DC_CNT                                                                                                           AS DC_CNT                    /* 할인건수 */
                          , NULL                                                                                                                AS FGF_PRESTAT_AMT           /* 사은품증정금액 */
                          , NULL                                                                                                                AS FGF_PRESTAT_CNT           /* 사은품증정건수 */
                          , A6.ACMLTMN_OFFR_CNT                                                                                                 AS ACMLTMN_OFFR_CNT          /* 적립금제공건수  */   
                          , NULL                                                                                                                AS DLR_ACMLTMN_OFFR_AMT      /* 원화적립금제공금액(달러)  */  
                          , NULL                                                                                                                AS ACMLTMN_USE_CNT           /* 적립금사용건수  */   
                          , NULL                                                                                                                AS ACMLTMN_OFFR_CUST         /* 적립금증정객수 */
                          , A6.WON_ACMLTMN_PYF_AMT                                                                                              AS WON_ACMLTMN_PYF_AMT       /* 원화적립금결제금액  */  
                          , (A6.WON_ACMLTMN_USE_NSALAMT + A6.WON_LDFP_ACMLT_AMT)                                                                   AS TOT_PRESTAT_AMT           /* 지원금액(전체)(=원화적립금사용순매출액 + 원화LDFPAY적립금액(증정금액)) */
                          , (A6.ACMLTMN_OURCOM_BDN_AMT + A6.LDFP_OURCOM_BDN_AMT)                                                                AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사)(=증정당사부담금액) */
                          , (A6.ACMLTMN_ALYCO_BDN_AMT  + A6.LDFP_ALYCO_BDN_AMT)                                                                 AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사)(=증정제휴사부담금액) */                     
                          , A6.DC_OURCOM_BDN_AMT                                                                                                AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
                          , A6.DC_ALYCO_BDN_AMT                                                                                                 AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */  
                          , A6.WON_PRMTN_DC_AMT                                                                                                 AS WON_TOT_DC_AMT            /* 원화총할인금액  */
                          , A6.WON_ARSE_NSALAMT                                                                                                 AS WON_ARSE_NSALAMT          /* 원화증정유발매출(원화유발순매출액) */
                          , A6.DLR_ARSE_NSALAMT                                                                                                 AS DLR_ARSE_NSALAMT          /* 달러증정유발매출(달러유발순매출액) */
                          , NULL                                                                                                                AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출 */
                          , NULL                                                                                                                AS ONLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출 */  
                          , A6.WON_ARSE_NSALAMT                                                                                                 AS OFLN_WON_ARSE_NSALAMT     /* 오프라인원화유발순매출액 (=원화유발순매출액) */ 
                          , A6.DLR_ARSE_NSALAMT                                                                                                 AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인달러유발순매출액 (=달러유발순매출액) */       
                          , A6.OMNI_WON_EXCH_NSALAMT                                                                                            AS OMNI_WON_EXCH_NSALAMT     /* 옴니원화교환권순매출액 */                     
                          , A6.OMNI_DLR_EXCH_NSALAMT                                                                                            AS OMNI_DLR_EXCH_NSALAMT     /* 옴니달러교환권순매출액 */
                          , A6.OMNI_WON_NSALAMT                                                                                                 AS OMNI_WON_NSALAMT          /* 옴니교환권순매출액 */       
                          , A6.OMNI_DLR_NSALAMT                                                                                                 AS OMNI_DLR_NSALAMT          /* 옴니달러순매출액 */
                          , A3.RGSTPSN_ID                                                                                                       AS RGSTPSN_ID                /* 등록자ID */
                          , A7.USR_NM                                                                                                           AS USR_NM                    /* 등록자명 */
                          , A3.RGST_DEPTCD                                                                                                      AS RGST_DEPTCD               /* 등록부서ID */
                          , A8.DEPT_NM                                                                                                          AS DEPT_NM                   /* 등록부서명 */
                       FROM LDF_DW.D_LGPROMO  A1 
                          , LDF_DW.D_MIDPROMO A2
                          , LDF_DW.D_PRMTN    A3
                          , LDF_DW.D_PRMTN_LGCSF A4
                          , LDF_DW.D_PRMTN_MDCSF A5
                          , LDF_DW.FE_MK_PRMTN_ACTRSLT A6  /* FE_MK_행사실적(온라인)  */
                          , LDF_DW.D_USR  A7
                          , LDF_DW.D_DEPT A8
                      WHERE 1=1
             ]]><if test='chnlTypeCd != null and chnlTypeCd == "02"'>
                            AND 1=2
                    </if>
                        AND A1.LGPRCD         = A2.LGPRCD 
                        AND A2.MDPRCD         = A3.MDPRCD 
                        AND A3.PRMTN_LGCSF_CD = A4.PRMTN_LGCSF_CD (+) 
                        AND A3.PRMTN_MDCSF_CD = A5.PRMTN_MDCSF_CD (+)
                        AND A3.PRMTNCD        = A6.PRMTNCD
                        AND A3.RGSTPSN_ID     = A7.EMPNO (+)
                        AND A3.RGST_DEPTCD    = A8.DEPTCD (+)
                            /* 조회조건 */
             <if test='prmtnDvsCd != null and prmtnDvsCd != "" and prmtncd != null and prmtncd != ""'>
                        <choose>
                            <when test='prmtnDvsCd == "1"'> <!-- 대행사 -->
                            AND A3.LGPRCD = #{prmtncd}
                            </when>
                            <when test='prmtnDvsCd == "2"'> <!-- 중행사 -->
                            AND A3.MDPRCD = #{prmtncd}
                            </when>
                            <when test='prmtnDvsCd == "3"'> <!-- 소행사 -->
                            AND A3.PRMTNCD = #{prmtncd}
                            </when>
                        </choose>
                    </if>
          <if test='inqryStrtDt != null and inqryStrtDt != ""'>
                            AND A6.STD_DT <![CDATA[>=]]> #{inqryStrtDt}  /* 조회 시작일 */
                    </if>
                    <if test='inqryEndDt != null and inqryEndDt != ""'>
                            AND A6.STD_DT <![CDATA[<=]]> #{inqryEndDt}  /* 조회 종료일 */
                    </if>
                    <if test='(langCd != null and langCd != "") or (langCds != null)'>
                            AND A3.PRMTNCD  IN (SELECT A3.PRMTNCD 
                                                  FROM LDF_DW.WL_LC_PRMTN_OFFR_CNDT A3
                                                 WHERE 1                      = 1
                                                   AND A3.PRMTN_OFFR_CNDT_CD  = '01'
                                                   AND A3.CHNL_TYPE_CD        = '01'
                    <if test='langCd != null and langCd != ""'>
                                                   AND A3.PRMTN_CNDT_VAL      = #{langCd} /* 조회조건 - 언어 */
                    </if>
                    <if test='langCds != null'>
                                                   AND A3.PRMTN_CNDT_VAL     IN <foreach collection="langCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 언어 */
                    </if>
                            )
                    </if><![CDATA[
                UNION ALL
                     SELECT /* FL_MK_PRMTN_ACTRSLT FL_MK_행사실적(오프라인) */  
                            A6.LGPRCD
                          , A1.LGPROMO_NM 
                          , A6.MDPRCD
                          , A2.MIDPROMO_NM 
                          , A6.PRMTNCD
                          , A3.PRMTN_NM 
                          , A3.ONOFF_DVS_CD                                                                                                       AS CHNL_TYPE_CD              /* 채널구분(1:오프라인,2:온라인) */
                          , A3.PRMTN_LGCSF_CD                                                                                                     AS PRMTN_LGCSF_CD            /* 행사대분류코드 */
                          , A4.PRMTN_LGCSF_NM                                                                                                     AS PRMTN_LGCSF_NM            /* 행사대분류코명 */ 
                          , A3.PRMTN_MDCSF_CD                                                                                                     AS PRMTN_MDCSF_CD            /* 행사중분류코드 */
                          , A5.PRMTN_MDCSF_NM                                                                                                     AS PRMTN_MDCSF_NM            /* 행사중분류명 */ 
                          , A3.PRMTN_STRT_DT                                                                                                      AS PRMTN_STRT_DT             /* 행사시작일자 */
                          , A3.PRMTN_END_DT                                                                                                       AS PRMTN_END_DT              /* 행사종료일자 */
                          , A6.INTG_MEMB_NO                                                                                                       AS INTG_MEMB_NO              /* 통합회원번호 */
                          , A6.WON_EXCH_NSALAMT                                                                                                   AS WON_EXCH_NSALAMT          /* 원화교환권순매출액 */
                          , A6.DLR_EXCH_NSALAMT                                                                                                   AS DLR_EXCH_NSALAMT          /* 달러교환권순매출액 */
                          , (CASE WHEN A6.DLR_EXCH_NSALAMT  IS NOT NULL THEN A6.INTG_MEMB_NO ELSE NULL END)                       AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
                          , A6.WON_PRMTN_NSALAMT                                                                                                  AS WON_PRMTN_NSALAMT         /* 원화행사순매출액 */
                          , A6.DLR_PRMTN_NSALAMT                                                                                                  AS DLR_PRMTN_NSALAMT         /* 달러행사순매출액 */
                          , (CASE WHEN A6.DLR_PRMTN_NSALAMT IS NOT NULL THEN A6.INTG_MEMB_NO ELSE NULL END)                       AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
                          , A6.LDFP_ACMLT_AMT                                                                                                     AS WON_LDFP_ACMLT_AMT        /* LDFPAY 적립금액 */
                          , A6.LDFP_ACMLT_CNT                                                                                                     AS LDFP_ACMLT_CNT            /* LDFPAY 적립건수 */
                          , A6.FREELDFP_USE_AMT                                                                                                   AS FREELDFP_USE_AMT          /* 프리LDFPAY사용금액 */
                          , A6.FREELDFP_USE_CNT                                                                                                   AS FREELDFP_USE_CNT          /* 프리LDFPAY사용건수 */
                          , A6.FREELDFP_ACMLT_AMT                                                                                                 AS FREELDFP_ACMLT_AMT        /* 프리LDFPAY증정금액(프리LDFPAY적립금액) */
                          , A6.FREELDFP_ACMLT_CNT                                                                                                 AS FREELDFP_ACMLT_CNT        /* 프리LDFPAY증정건수(프리LDFPAY적립건수) */ 
                          , A6.DC_CNT                                                                                                             AS DC_CNT                    /* 할인건수 */
                          , A6.FGF_PRESTAT_AMT                                                                                                    AS FGF_PRESTAT_AMT           /* 사은품증정금액 */
                          , A6.FGF_PRESTAT_CNT                                                                                                    AS FGF_PRESTAT_CNT           /* 사은품증정건수 */
                          , NULL                                                                                                                  AS ACMLTMN_OFFR_CNT          /* 적립금제공건수  */   
                          , NULL                                                                                                                  AS DLR_ACMLTMN_OFFR_AMT      /* 원화적립금제공금액(달러)  */  
                          , NULL                                                                                                                  AS ACMLTMN_USE_CNT           /* 적립금사용건수  */  
                          , NULL                                                                                                                  AS ACMLTMN_OFFR_CUST         /* 적립금증정객수 */
                          , NULL                                                                                                                  AS WON_ACMLTMN_PYF_AMT       /* 원화적립금결제금액  */ 
                          , (A6.FGF_PRESTAT_AMT + A6.LDFP_ACMLT_AMT + A6.FREELDFP_USE_AMT)                                                        AS TOT_PRESTAT_AMT           /* 지원금액(전체)(=사은품증정금액 + LDFPAY적립금액 + 프리LDFPAY사용금액) */
                          , A6.PRESTAT_OURCOM_BDN_AMT                                                                                             AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사)(=증정당사부담금액) */
                          , A6.PRESTAT_ALYCO_BDN_AMT                                                                                              AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사)(=증정제휴사부담금액) */                     
                          , A6.DC_OURCOM_BDN_AMT                                                                                                  AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
                          , A6.DC_ALYCO_BDN_AMT                                                                                                   AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */  
                          , A6.WON_PRMTN_DC_AMT                                                                                                     AS WON_TOT_DC_AMT            /* 원화총할인금액(원화행사할인금액)  */
                          , A6.WON_ARSE_NSALAMT                                                                                                   AS WON_ARSE_NSALAMT          /* 증정유발매출(원화유발순매출액) */
                          , A6.DLR_ARSE_NSALAMT                                                                                                   AS DLR_ARSE_NSALAMT          /* 달러증정유발매출(달러유발순매출액) */
                          , A6.WON_CROS_ARSE_NSALAMT                                                                                              AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출 */
                          , A6.DLR_CROS_ARSE_NSALAMT                                                                                              AS ONLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출 */  
                          , A6.WON_ARSE_NSALAMT                                                                                                                  AS OFLN_WON_ARSE_NSALAMT     /* 오프라인원화유발순매출액 (=원화교차유발순매출액) */ 
                          , A6.DLR_ARSE_NSALAMT                                                                                                                  AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인달러유발순매출액 (=달러교차유발순매출액) */       
                          , A6.OMNI_WON_EXCH_NSALAMT                                                                                              AS OMNI_WON_EXCH_NSALAMT     /* 옴니원화교환권순매출액 */                     
                          , A6.OMNI_DLR_EXCH_NSALAMT                                                                                              AS OMNI_DLR_EXCH_NSALAMT     /* 옴니달러교환권순매출액 */
                          , A6.OMNI_WON_NSALAMT                                                                                                   AS OMNI_WON_NSALAMT          /* 옴니교환권순매출액 */       
                          , A6.OMNI_DLR_NSALAMT                                                                                                   AS OMNI_DLR_NSALAMT          /* 옴니달러순매출액 */
                          , A3.RGSTPSN_ID                                                                                                         AS RGSTPSN_ID                /* 등록자ID */
                          , A7.USR_NM                                                                                                             AS USR_NM                    /* 등록자명 */
                          , A3.RGST_DEPTCD                                                                                                        AS RGST_DEPTCD               /* 등록부서ID */
                          , A8.DEPT_NM                                                                                                            AS DEPT_NM                   /* 등록부서명 */
                       FROM LDF_DW.D_LGPROMO  A1 
                          , LDF_DW.D_MIDPROMO A2
                          , LDF_DW.D_PRMTN    A3
                          , LDF_DW.D_PRMTN_LGCSF A4
                          , LDF_DW.D_PRMTN_MDCSF A5
                          , LDF_DW.FL_MK_PRMTN_ACTRSLT A6  /* FL_MK_행사실적(오프라인)  */
                          , LDF_DW.D_USR  A7
                          , LDF_DW.D_DEPT A8
                      WHERE 1=1
             ]]><if test='chnlTypeCd != null and chnlTypeCd == "01"'>
                            AND 1=2
                    </if>
                        AND A1.LGPRCD         = A2.LGPRCD 
                        AND A2.MDPRCD         = A3.MDPRCD 
                        AND A3.PRMTN_LGCSF_CD = A4.PRMTN_LGCSF_CD (+)
                        AND A3.PRMTN_MDCSF_CD = A5.PRMTN_MDCSF_CD (+)
                        AND A3.PRMTNCD        = A6.PRMTNCD 
                        AND A3.RGSTPSN_ID     = A7.EMPNO (+)
                        AND A3.RGST_DEPTCD    = A8.DEPTCD (+)
                            /* 조회조건 */
         <if test='prmtnDvsCd != null and prmtnDvsCd != "" and prmtncd != null and prmtncd != ""'>
                        <choose>
                            <when test='prmtnDvsCd == "1"'> <!-- 대행사 -->
                            AND A3.LGPRCD = #{prmtncd}
                            </when>
                            <when test='prmtnDvsCd == "2"'> <!-- 중행사 -->
                            AND A3.MDPRCD = #{prmtncd}
                            </when>
                            <when test='prmtnDvsCd == "3"'> <!-- 소행사 -->
                            AND A3.PRMTNCD = #{prmtncd}
                            </when>
                        </choose>
                    </if>
          <if test='inqryStrtDt != null and inqryStrtDt != ""'>
                            AND A6.STD_DT <![CDATA[>=]]> #{inqryStrtDt}  /* 조회 시작일 */
                    </if>
                    <if test='inqryEndDt != null and inqryEndDt != ""'>
                            AND A6.STD_DT <![CDATA[<=]]> #{inqryEndDt}  /* 조회 종료일 */
                    </if>
                    <if test='(strCd != null and strCd != "") or (strCds != null) or (biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != "") or (biznsStrLoctnDvsCds != null)'>
                            AND A3.PRMTNCD  IN (SELECT PRMTNCD
                                                  FROM LDF_DW.WL_LC_PRMTN_OFFR_CNDT A3
                                                     , LDF_DW.D_STR A4
                                                 WHERE 1                          = 1
                                                   AND A3.PRMTN_CNDT_VAL          = A4.STR_CD
                                                   AND A3.PRMTN_OFFR_CNDT_CD      = '01'
                                                   AND A3.CHNL_TYPE_CD            = '02'
                        <if test='strCd != null and strCd != ""'>
                                                   AND A3.PRMTN_CNDT_VAL          = #{strCd} /* 조회조건 - 지점 */
                        </if>
                        <if test='strCds != null'>
                                                   AND A3.PRMTN_CNDT_VAL         IN <foreach collection="strCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 지점 */
                        </if>
                        <if test='biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != ""'>
                                                   AND A4.BIZNS_STR_LOCTN_DVS_CD  = #{biznsStrLoctnDvsCd} /* 조회조건 - 영업점위치구분 */
                        </if>
                        <if test='biznsStrLoctnDvsCds != null'>
                                                   AND A4.BIZNS_STR_LOCTN_DVS_CD IN <foreach collection="biznsStrLoctnDvsCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 영업점위치구분 */
                        </if>
                              )
                    </if><![CDATA[
                      UNION ALL
                     SELECT /* FE_MK_혜택OFFER상세 */
                            A1.LGPRCD                                                 AS LGPRCD
                          , A2.LGPROMO_NM                                             AS LGPROMO_NM
                          , A1.MDPRCD                                                 AS MDPRCD
                          , A3.MIDPROMO_NM                                            AS MIDPROMO_NM
                          , A1.PRMTNCD                                                AS PRMTNCD 
                          , A1.PRMTN_NM                                               AS PRMTN_NM
                          , A1.ONOFF_DVS_CD                                           AS CHNL_TYPE_CD
                          , A1.PRMTN_LGCSF_CD                                         AS PRMTN_LGCSF_CD
                          , A4.PRMTN_LGCSF_NM                                         AS PRMTN_LGCSF_NM
                          , A1.PRMTN_MDCSF_CD                                         AS PRMTN_MDCSF_CD
                          , A5.PRMTN_MDCSF_NM                                         AS PRMTN_MDCSF_NM
                          , A1.PRMTN_STRT_DT                                          AS PRMTN_STRT_DT
                          , A1.PRMTN_END_DT                                           AS PRMTN_END_DT
                          , NULL                                                      AS INTG_MEMB_NO              /* 통합회원번호 */
                          , 0                                                         AS WON_EXCH_NSALAMT          /* 원화교환권순매출액 */
                          , 0                                                         AS DLR_EXCH_NSALAMT          /* 달러교환권순매출액 */
                          , NULL                                                      AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
                          , 0                                                         AS WON_PRMTN_NSALAMT         /* 원화행사순매출액 */
                          , 0                                                         AS DLR_PRMTN_NSALAMT         /* 달러행사순매출액 */
                          , NULL                                                      AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
                          , 0                                                         AS WON_LDFP_ACMLT_AMT        /* LDFPAY 적립금액 */
                          , 0                                                         AS LDFP_ACMLT_CNT            /* LDFPAY 적립건수 */
                          , 0                                                         AS FREELDFP_USE_AMT          /* 프리LDFPAY사용금액 */
                          , 0                                                         AS FREELDFP_USE_CNT          /* 프리LDFPAY사용건수 */
                          , 0                                                         AS FREELDFP_ACMLT_AMT        /* 프리LDFPAY증정금액(프리LDFPAY적립금액) */
                          , 0                                                         AS FREELDFP_ACMLT_CNT        /* 프리LDFPAY증정건수(프리LDFPAY적립건수) */ 
                          , 0                                                         AS DC_CNT                    /* 할인건수 */
                          , 0                                                         AS FGF_PRESTAT_AMT           /* 사은품증정금액 */
                          , 0                                                         AS FGF_PRESTAT_CNT           /* 사은품증정건수 */
                          , 0                                                         AS ACMLTMN_OFFR_CNT          /* 적립금제공건수 */
                          , A1.DLR_ACMLTMN_OFFR_AMT                                   AS DLR_ACMLTMN_OFFR_AMT      /* 적립금제공금액(달러) */
                          , A1.ACMLTMN_OFFR_CNT                                       AS ACMLTMN_USE_CNT           /* 적립금사용건수 */
                          , A1.ACMLTMN_OFFR_CUST_NBR                                  AS ACMLTMN_OFFR_CUST         /* 적립금증정객수 */
                          , 0                                                         AS WON_ACMLTMN_PYF_AMT       /* 원화적립금결제금액  */  
                          , 0                                                         AS TOT_PRESTAT_AMT           /* 지원금액(전체)(=당사+제휴사) */
                          , 0                                                         AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사)(=증정당사부담금액) */
                          , 0                                                         AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사)(=증정제휴사부담금액) */                     
                          , 0                                                         AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
                          , 0                                                         AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */  
                          , 0                                                         AS WON_TOT_DC_AMT            /* 원화총할인금액  */ 
                          , 0                                                         AS WON_ARSE_NSALAMT          /* 증정유발매출(원화유발순매출액) */
                          , 0                                                         AS DLR_ARSE_NSALAMT          /* 달러증정유발매출(달러유발순매출액) */
                          , 0                                                         AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출 */
                          , 0                                                         AS ONLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출 */  
                          , 0                                                         AS OFLN_WON_ARSE_NSALAMT     /* 오프라인원화유발순매출액 (=원화교차유발순매출액) */ 
                          , 0                                                         AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인달러유발순매출액 (=달러교차유발순매출액) */       
                          , 0                                                         AS OMNI_WON_EXCH_NSALAMT     /* 옴니원화교환권순매출액 */                     
                          , 0                                                         AS OMNI_DLR_EXCH_NSALAMT     /* 옴니달러교환권순매출액 */
                          , 0                                                         AS OMNI_WON_NSALAMT          /* 옴니교환권순매출액 */       
                          , 0                                                         AS OMNI_DLR_NSALAMT          /* 옴니달러순매출액 */
                          , A1.RGSTPSN_ID                                             AS RGSTPSN_ID                /* 등록자ID */
                          , A6.USR_NM                                                 AS USR_NM                    /* 등록자명 */
                          , A1.RGST_DEPTCD                                            AS RGST_DEPTCD               /* 등록부서ID */
                          , A7.DEPT_NM                                                AS DEPT_NM                   /* 등록부서명 */
                       FROM (SELECT /*+ PARALLEL(4) USE_HASH(B1,B4)  FULL(B1) FULL(B4)*/
                                    B1.PRMTNCD                      AS PRMTNCD 
                                  , B1.PRMTN_NM                     AS PRMTN_NM
                                  , MAX(B1.LGPRCD)                  AS LGPRCD
                                  , MAX(B1.MDPRCD)                  AS MDPRCD
                                  , MAX(B1.PRMTN_STRT_DT)           AS PRMTN_STRT_DT 
                                  , MAX(B1.PRMTN_END_DT)            AS PRMTN_END_DT 
                                  , MAX(B1.ONOFF_DVS_CD)            AS ONOFF_DVS_CD 
                                  , MAX(B1.PRMTN_LGCSF_CD)          AS PRMTN_LGCSF_CD 
                                  , MAX(B1.PRMTN_MDCSF_CD)          AS PRMTN_MDCSF_CD 
                                  , MAX(B1.RGSTPSN_ID)              AS RGSTPSN_ID 
                                  , MAX(B1.RGST_DEPTCD)             AS RGST_DEPTCD 
                                  , COUNT(DISTINCT B4.INTG_MEMB_NO) AS ACMLTMN_OFFR_CUST_NBR /* 적립금제공건수 */
                                  , SUM(B4.OFFER_PRESTAT_CNT)       AS ACMLTMN_OFFR_CNT /* 적립금증정_건수 */
                                  , SUM(B4.DLR_OFFER_PRESTAT_AMT)   AS DLR_ACMLTMN_OFFR_AMT /* 달러적립금제공금액 */        
                               FROM (SELECT B3.PRMTNCD
                                          , B3.PRMTN_NM
                                          , B3.LGPRCD
                                          , B3.MDPRCD
                                          , B3.PRMTN_STRT_DT
                                          , B3.PRMTN_END_DT
                                          , B3.ONOFF_DVS_CD
                                          , B3.PRMTN_LGCSF_CD
                                          , B3.PRMTN_MDCSF_CD
                                          , B3.RGSTPSN_ID
                                          , B3.RGST_DEPTCD
                                          , B6.BNFT_OBJ_NO      AS ONLN_OFFER_NO  /* 혜택대상번호 */
                                          , B6.PRMTN_BNFT_NO    AS PRMTN_BNFT_NO  /* 행사혜택번호 */
                                       FROM LDF_DW.D_PRMTN B3
                                          , LDF_DW.WL_LC_PRMTN_BNFT B6
                                      WHERE 1=1
                        AND B3.PRMTNCD        = B6.PRMTNCD 
                                        AND B3.PRMTN_LGCSF_CD = '007' /* 적립금 */
             ]]><if test='prmtnDvsCd != null and prmtnDvsCd != "" and prmtncd != null and prmtncd != ""'>
                        <choose>
                            <when test='prmtnDvsCd == "1"'> <!-- 대행사 -->
                                            AND B3.LGPRCD         = #{prmtncd} /* 선택조건 - 대행사코드 */
                            </when>
                            <when test='prmtnDvsCd == "2"'> <!-- 중행사 -->
                                            AND B3.MDPRCD         = #{prmtncd}       /* 선택조건 - 중행사코드 */
                            </when>
                            <when test='prmtnDvsCd == "3"'> <!-- 소행사 -->
                                            AND B3.PRMTNCD        = #{prmtncd}      /* 선택조건 - 소행사코드 */
                            </when>
                        </choose>
                    </if>
                                    ) B1
                                  , LDF_DW.FE_MK_BNFT_OFFER_DTL B4
                              WHERE 1=1
                                AND B4.STD_DT       BETWEEN B1.PRMTN_STRT_DT AND B1.PRMTN_END_DT 
                                AND B4.STD_TM_VAL        >= '00'
                                AND B4.ONLN_OFFER_NO      = B1.ONLN_OFFER_NO 
                                AND B4.PRMTN_BNFT_NO      = B1.PRMTN_BNFT_NO 
                                AND B4.OFFER_PRESTAT_CNT <![CDATA[<>]]> 0 /* 적립건수 0이 아닌경우 */
                                
          <if test='inqryStrtDt != null and inqryStrtDt != ""'>
                                    AND B4.STD_DT <![CDATA[>=]]> #{inqryStrtDt}  /* 조회 시작일 */
                    </if>
                    <if test='inqryEndDt != null and inqryEndDt != ""'>
                                    AND B4.STD_DT <![CDATA[<=]]> #{inqryEndDt}  /* 조회 종료일 */
                    </if>
                              GROUP BY B1.PRMTNCD
                                     , B1.PRMTN_NM
                            ) A1
                          , LDF_DW.D_LGPROMO A2
                          , LDF_DW.D_MIDPROMO A3
                          , LDF_DW.D_PRMTN_LGCSF A4
                          , LDF_DW.D_PRMTN_MDCSF A5
                          , LDF_DW.D_USR A6
                          , LDF_DW.D_DEPT A7
                      WHERE 1=1
                        AND A1.LGPRCD         = A2.LGPRCD 
                        AND A1.MDPRCD         = A3.MDPRCD
                        AND A1.PRMTN_LGCSF_CD = A4.PRMTN_LGCSF_CD (+)
                        AND A1.PRMTN_MDCSF_CD = A5.PRMTN_MDCSF_CD (+)
                        AND A1.RGSTPSN_ID     = A6.EMPNO (+)
                        AND A1.RGST_DEPTCD    = A7.DEPTCD (+)
            <if test='(strCd != null and strCd != "") or (strCds != null) or (biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != "") or (biznsStrLoctnDvsCds != null)'>
                            AND A1.PRMTNCD      IN (SELECT PRMTNCD 
                                                      FROM LDF_DW.WL_LC_PRMTN_OFFR_CNDT A3
                                                         , LDF_DW.D_STR A4
                                                     WHERE 1                          = 1
                                                       AND A3.PRMTN_CNDT_VAL          = A4.STR_CD
                                                       AND A3.PRMTN_OFFR_CNDT_CD      = '01'
                                                       AND A3.CHNL_TYPE_CD            = '02'
                        <if test='strCd != null and strCd != ""'>
                                                       AND A3.PRMTN_CNDT_VAL          = #{strCd} /* 조회조건 - 지점 */
                        </if>
                        <if test='strCds != null'>
                                                       AND A3.PRMTN_CNDT_VAL         IN <foreach collection="strCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 지점 */
                        </if>
                        <if test='biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != ""'>
                                                       AND A4.BIZNS_STR_LOCTN_DVS_CD  = #{biznsStrLoctnDvsCd} /* 조회조건 - 영업점위치구분 */
                        </if>
                        <if test='biznsStrLoctnDvsCds != null'>
                                                       AND A4.BIZNS_STR_LOCTN_DVS_CD IN <foreach collection="biznsStrLoctnDvsCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 영업점위치구분 */
                        </if>
                                                    )
                    </if>
                    ) T
              GROUP BY T.LGPRCD                                                                                                                                                          
                  , T.LGPROMO_NM                                                                                                                                                     
                  , T.MDPRCD                                                                                                                                                          
                  , T.MIDPROMO_NM                                                                                                                                                    
                  , T.PRMTNCD                                                                                                                                                         
                  , T.PRMTN_NM        
           ) M
           ;
           
           
--2. 구간대별
SELECT M.LGPRCD                              AS LGPRCD                    /* 대행사코드 */
              , M.LGPROMO_NM                          AS LGPROMO_NM                /* 대행사명 */
              , M.MDPRCD                              AS MDPRCD                    /* 중행사코드 */
              , M.MIDPROMO_NM                         AS MIDPROMO_NM               /* 중행사명 */
              , M.PRMTNCD                             AS PRMTNCD                   /* 소행사코드 */
              , M.PRMTN_NM                            AS PRMTN_NM                  /* 소행사명 */
              , M.STR_CD                              AS STR_LANG_CD               /* 지점/언어코드 */
              , (SELECT B1.STR_NM
                   FROM LDF_DW.D_STR B1
                  WHERE B1.STR_CD = M.STR_CD
                 )                                     AS STR_LANG_NM               /* 지점/언어명 */
              , M.PRMTN_LGCSF_CD                       AS PRMTN_LGCSF_CD            /* 행사대분류코드 */
              , M.PRMTN_LGCSF_NM                       AS PRMTN_LGCSF_NM            /* 행사대분류명 */
              , M.PRMTN_MDCSF_CD                       AS PRMTN_MDCSF_CD            /* 행사중분류코드 */
              , M.PRMTN_MDCSF_NM                       AS PRMTN_MDCSF_NM            /* 행사대분류명 */
              , M.PRMTN_APLY_STRT_AMT                  AS PRMTN_APLY_STRT_AMT           /* 구간대금액 */
              , M.PRMTN_STRT_DT                        AS PRMTN_STRT_DT             /* 행사시작일자 */
              , M.PRMTN_END_DT                         AS PRMTN_END_DT              /* 행사종료일자 */
              , NVL(M.WON_EXCH_NSALAMT,0)              AS WON_EXCH_NSALAMT          /* 교환권매출_실적(원화) */
              , NVL(M.DLR_EXCH_NSALAMT,0)              AS DLR_EXCH_NSALAMT          /* 교환권매출_실적(달러) */
              , NVL(M.EXCH_SALES_CNT,0)                AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
              , NVL(M.WON_ARSE_NSALAMT_CUSTRN,0)       AS WON_ARSE_NSALAMT_CUSTRN   /* 교환권매출_객단가(원화) */
              , NVL(M.DLR_ARSE_NSALAMT_CUSTRN,0)       AS DLR_ARSE_NSALAMT_CUSTRN   /* 교환권매출_객단가(달러) */
              , NVL(M.WON_PRMTN_NSALAMT,0)             AS WON_PRMTN_NSALAMT         /* 행사매출_실적(원화) */
              , NVL(M.DLR_PRMTN_NSALAMT,0)             AS DLR_PRMTN_NSALAMT         /* 행사매출_실적(달러) */
              , NVL(M.PRMTN_NSALAMT_CNT,0)             AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
              , NVL(M.WON_PRMTN_NSALAMT_CUSTRN,0)      AS WON_PRMTN_NSALAMT_CUSTRN  /* 행사매출_객단가(원화) */
              , NVL(M.DLR_PRMTN_NSALAMT_CUSTRN,0)      AS DLR_PRMTN_NSALAMT_CUSTRN  /* 행사매출_객단가(달러) */
              , NVL(M.WON_LDFP_ACMLT_AMT,0)            AS WON_LDFP_ACMLT_AMT        /* LDFPAY_증정금액(원화) */
              , NVL(M.LDFP_ACMLT_CNT,0)                AS LDFP_ACMLT_CNT            /* LDFPAY_증정건수 */
              , NVL(M.LDFP_ACMLT_CUST_NBR,0)           AS LDFP_ACMLT_CUST_NBR       /* LDFPAY_증정객수 */
              , NVL(M.FREELDFP_USE_AMT,0)              AS FREELDFP_USE_AMT          /* FREE_LDFPAY_사용금액 */
              , NVL(M.FREELDFP_USE_CNT,0)              AS FREELDFP_USE_CNT          /* FREE_LDFPAY_사용건수 */
              , NVL(M.FREELDFP_USE_CUST_NBR,0)         AS FREELDFP_USE_CUST_NBR     /* FREE_LDFPAY_사용객수 */
              , NVL(M.FREELDFP_ACMLT_AMT,0)            AS FREELDFP_ACMLT_AMT        /* FREE_LDFPAY_증정금액 */
              , NVL(M.FREELDFP_ACMLT_CNT,0)            AS FREELDFP_ACMLT_CNT        /* FREE_LDFPAY_증정건수 */
              , NVL(M.FREELDFP_ACLMLT_CUST_NBR,0)      AS FREELDFP_ACLMLT_CUST_NBR  /* FREE_LDFPAY_증정객수 */
              , NVL(M.DC_AMT,0)                        AS DC_AMT                    /* 할인_금액 */
              , NVL(M.DC_CNT,0)                        AS DC_CNT                    /* 할인_건수 */
              , NVL(M.BDN_CUST_NBR,0)                  AS BDN_CUST_NBR              /* 할인_객수 */
              , NVL(M.FGF_PRESTAT_AMT,0)               AS FGF_PRESTAT_AMT           /* 사은품증정_금액 */
              , NVL(M.FGF_PRESTAT_CNT,0)               AS FGF_PRESTAT_CNT           /* 사은품증정_건수 */
              , NVL(M.FGF_PRESTAT_CUST_NBR,0)          AS FGF_PRESTAT_CUST_NBR      /* 사은품증정_객수 */
              , NVL(M.ACMLTMN_OFFR_CNT,0)              AS ACMLTMN_OFFR_CNT          /* 적립금증정_건수 */
              , NVL(M.DLR_ACMLTMN_OFFR_AMT,0)          AS DLR_ACMLTMN_OFFR_AMT      /* 적립금증정_금액(원화) */
              , NVL(M.ACMLTMN_OFFR_CUST_NBR,0)         AS ACMLTMN_OFFR_CUST_NBR     /* 적립금증정_객수 */
              , NVL(M.ACMLTMN_USE_CNT,0)               AS ACMLTMN_USE_CNT           /* 적립금사용_건수 */
              , NVL(M.WON_ACMLTMN_PYF_AMT,0)           AS WON_ACMLTMN_PYF_AMT       /* 적립금사용_금액(원화) */
              , NVL(M.ACMLTMN_USE_CUST_NBR,0)          AS ACMLTMN_USE_CUST_NBR      /* 적립금사용_객수 */
              , NVL(M.TOT_PRESTAT_AMT,0)               AS TOT_PRESTAT_AMT           /* 지원금액(전체) */
              , NVL(M.PRESTAT_OURCOM_BDN_AMT,0)        AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사) */
              , NVL(M.PRESTAT_ALYCO_BDN_AMT,0)         AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사) */
              , NVL(M.TOT_PRESTAT_RATE,0)              AS OURCOM_PRESTAT_RATE          /* 지원율(전체) */
              , NVL(M.OURCOM_PRESTAT_RATE,0)           AS TOT_PRESTAT_RATE       /* 지원율(당사) */     
              , NVL(M.TOT_BDN_AMT,0)                   AS TOT_BDN_AMT               /* 할인금액(전체) */
              , NVL(M.DC_OURCOM_BDN_AMT,0)             AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
              , NVL(M.DC_ALYCO_BDN_AMT,0)              AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */
              , NVL(M.TOT_DC_CNT,0)                    AS TOT_DC_CNT                /* 할인건수 */
              , NVL(M.WON_TOT_DC_AMT,0)                AS WON_TOT_DC_AMT            /* 원화총할인금액 */
              , NVL(M.DC_TOT_BDN_RATE,0)               AS DC_OURCOM_BDN_RATE           /* 할인율(전체) */
              , NVL(M.DC_OURCOM_BDN_RATE,0)            AS DC_TOT_BDN_RATE        /* 할인율(당사) */     
		     , NVL(M.TOT_PRESTAT_AMT+M.TOT_BDN_AMT,0) AS PRMTN_EXP                 /* 행사비용(증정금액+할인금액) */
		     , NVL(ROUND(((M.TOT_PRESTAT_AMT + M.TOT_BDN_AMT) / DECODE(M.WON_PRMTN_NSALAMT, 0, NULL, M.WON_PRMTN_NSALAMT))*100,1),0) AS INTG_SUPTRT   /* 통합지원율(증정금액+할인금액) / 원화행사순매출액 */ 
              , NVL(M.WON_ARSE_NSALAMT,0)              AS WON_ARSE_NSALAMT          /* 증정유발매출(원화) */
              , NVL(M.ONLN_WON_ARSE_NSALAMT,0)         AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출(원화) */
              , NVL(M.ONLN_DLR_ARSE_NSALAMT,0)         AS ONLN_DLR_ARSE_NSALAMT     /* 온라인유발매출(달러) */
              , NVL(M.OFLN_WON_ARSE_NSALAMT,0)         AS OFLN_WON_ARSE_NSALAMT     /* 오프라인유발매출(원화) */
              , NVL(M.OFLN_DLR_ARSE_NSALAMT,0)         AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출(달러) */
              , NVL(M.OMNI_WON_EXCH_NSALAMT,0)         AS OMNI_WON_EXCH_NSALAMT     /* 교환권옴니매출(원화) */
              , NVL(M.OMNI_DLR_EXCH_NSALAMT,0)         AS OMNI_DLR_EXCH_NSALAMT     /* 교환권옴니매출(달러) */
              , NVL(M.OMNI_WON_NSALAMT,0)              AS OMNI_WON_NSALAMT          /* 행사옴니매출(원화) */
              , NVL(M.OMNI_DLR_NSALAMT,0)              AS OMNI_DLR_NSALAMT          /* 행사옴니매출(달러) */
              , M.RGSTPSN_ID                           AS RGSTPSN_ID                /* 등록자ID */
              , M.USR_NM                               AS RGSTPSN_NM                    /* 등록자명 */
              , M.RGST_DEPTCD                          AS RGST_DEPTCD               /* 등록부서코드 */
              , M.DEPT_NM                              AS RGST_DEPT_NM                   /* 등록부서명 */
           FROM (SELECT A.LGPRCD                              AS LGPRCD                    /* 대행사코드 */
                      , A.LGPROMO_NM                          AS LGPROMO_NM                /* 대행사명 */
                      , A.MDPRCD                              AS MDPRCD                    /* 중행사코드 */
                      , A.MIDPROMO_NM                         AS MIDPROMO_NM               /* 중행사명 */
                      , A.PRMTNCD                             AS PRMTNCD                   /* 소행사코드 */
                      , A.PRMTN_NM                            AS PRMTN_NM                  /* 소행사명 */
                      , A.STR_CD                              AS STR_CD                   /* 지점코드 */
                      , A.PRMTN_APLY_STRT_AMT            AS PRMTN_APLY_STRT_AMT           /* 구간대금액 */
                      , MAX(A.PRMTN_LGCSF_CD)                 AS PRMTN_LGCSF_CD            /* 행사대분류코드 */
                      , MAX(A.PRMTN_LGCSF_NM)                 AS PRMTN_LGCSF_NM            /* 행사대분류명 */
                      , MAX(A.PRMTN_MDCSF_CD)                 AS PRMTN_MDCSF_CD            /* 행사중분류코드 */
                      , MAX(A.PRMTN_MDCSF_NM)                 AS PRMTN_MDCSF_NM            /* 행사대분류명 */
                      , MAX(A.CHNL_TYPE_CD)                   AS CHNL_TYPE_CD              /* 채널 구분 */
                      , MAX(A.PRMTN_STRT_DT)                  AS PRMTN_STRT_DT             /* 행사시작일자 */
                      , MAX(A.PRMTN_END_DT)                   AS PRMTN_END_DT              /* 행사종료일자 */
                      , MAX(A.WON_EXCH_NSALAMT)               AS WON_EXCH_NSALAMT          /* 교환권매출_실적(원화) */
                      , MAX(A.DLR_EXCH_NSALAMT)               AS DLR_EXCH_NSALAMT          /* 교환권매출_실적(달러) */
                      , MAX(A.EXCH_SALES_CNT)                 AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
                      , MAX(A.WON_ARSE_NSALAMT_CUSTRN)        AS WON_ARSE_NSALAMT_CUSTRN   /* 교환권매출_객단가(원화) */
                      , MAX(A.DLR_ARSE_NSALAMT_CUSTRN)        AS DLR_ARSE_NSALAMT_CUSTRN   /* 교환권매출_객단가(달러) */
                      , MAX(A.WON_PRMTN_NSALAMT)              AS WON_PRMTN_NSALAMT         /* 행사매출_실적(원화) */
                      , MAX(A.DLR_PRMTN_NSALAMT)              AS DLR_PRMTN_NSALAMT         /* 행사매출_실적(달러) */
                      , MAX(A.PRMTN_NSALAMT_CNT)              AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
                      , MAX(A.WON_PRMTN_NSALAMT_CUSTRN)       AS WON_PRMTN_NSALAMT_CUSTRN  /* 행사매출_객단가(원화) */
                      , MAX(A.DLR_PRMTN_NSALAMT_CUSTRN)       AS DLR_PRMTN_NSALAMT_CUSTRN  /* 행사매출_객단가(달러) */
                      , MAX(A.WON_LDFP_ACMLT_AMT)             AS WON_LDFP_ACMLT_AMT        /* LDFPAY_증정금액(원화) */
                      , MAX(A.LDFP_ACMLT_CNT)                 AS LDFP_ACMLT_CNT            /* LDFPAY_증정건수 */
                      , MAX(A.LDFP_ACMLT_CUST_NBR)            AS LDFP_ACMLT_CUST_NBR       /* LDFPAY_증정객수 */
                      , MAX(A.FREELDFP_USE_AMT)               AS FREELDFP_USE_AMT          /* FREE_LDFPAY_사용금액 */
                      , MAX(A.FREELDFP_USE_CNT)               AS FREELDFP_USE_CNT          /* FREE_LDFPAY_사용건수 */
                      , MAX(A.FREELDFP_USE_CUST_NBR)          AS FREELDFP_USE_CUST_NBR     /* FREE_LDFPAY_사용객수 */
                      , MAX(A.FREELDFP_ACMLT_AMT)             AS FREELDFP_ACMLT_AMT        /* FREE_LDFPAY_증정금액 */
                      , MAX(A.FREELDFP_ACMLT_CNT)             AS FREELDFP_ACMLT_CNT        /* FREE_LDFPAY_증정건수 */
                      , MAX(A.FREELDFP_ACLMLT_CUST_NBR)       AS FREELDFP_ACLMLT_CUST_NBR  /* FREE_LDFPAY_증정객수 */
                      , MAX(A.DC_AMT)                         AS DC_AMT                    /* 할인_금액 */
                      , MAX(A.DC_CNT)                         AS DC_CNT                    /* 할인_건수 */
                      , MAX(A.BDN_CUST_NBR)                   AS BDN_CUST_NBR              /* 할인_객수 */
                      , MAX(A.FGF_PRESTAT_AMT)                AS FGF_PRESTAT_AMT           /* 사은품증정_금액 */
                      , MAX(A.FGF_PRESTAT_CNT)                AS FGF_PRESTAT_CNT           /* 사은품증정_건수 */
                      , MAX(A.FGF_PRESTAT_CUST_NBR)           AS FGF_PRESTAT_CUST_NBR      /* 사은품증정_객수 */
                      , MAX(A.ACMLTMN_OFFR_CNT)               AS ACMLTMN_OFFR_CNT          /* 적립금증정_건수 */
                      , MAX(A.DLR_ACMLTMN_OFFR_AMT)           AS DLR_ACMLTMN_OFFR_AMT      /* 적립금증정_금액(원화) */
                      , MAX(A.ACMLTMN_OFFR_CUST_NBR)          AS ACMLTMN_OFFR_CUST_NBR     /* 적립금증정_객수 */
                      , MAX(A.ACMLTMN_USE_CNT)                AS ACMLTMN_USE_CNT           /* 적립금사용_건수 */
                      , MAX(A.WON_ACMLTMN_PYF_AMT)            AS WON_ACMLTMN_PYF_AMT       /* 적립금사용_금액(원화) */
                      , MAX(A.ACMLTMN_USE_CUST_NBR)           AS ACMLTMN_USE_CUST_NBR      /* 적립금사용_객수 */
                      , MAX(A.TOT_PRESTAT_AMT)                AS TOT_PRESTAT_AMT           /* 지원금액(전체) */
                      , MAX(A.PRESTAT_OURCOM_BDN_AMT)         AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사) */
                      , MAX(A.PRESTAT_ALYCO_BDN_AMT)          AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사) */
                      , MAX(A.TOT_PRESTAT_RATE)               AS TOT_PRESTAT_RATE          /* 지원율(전체) */
                      , MAX(A.OURCOM_PRESTAT_RATE)            AS OURCOM_PRESTAT_RATE       /* 지원율(당사) */     
                      , MAX(A.TOT_BDN_AMT)                    AS TOT_BDN_AMT               /* 할인금액(전체) */
                      , MAX(A.DC_OURCOM_BDN_AMT)              AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
                      , MAX(A.DC_ALYCO_BDN_AMT)               AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */
                      , MAX(A.TOT_DC_CNT)                     AS TOT_DC_CNT                /* 할인건수 */
                      , MAX(A.WON_TOT_DC_AMT)                 AS WON_TOT_DC_AMT            /* 원화총할인금액 */
                      , MAX(A.DC_TOT_BDN_RATE)                AS DC_TOT_BDN_RATE           /* 할인율(전체) */
                      , MAX(A.DC_OURCOM_BDN_RATE)             AS DC_OURCOM_BDN_RATE        /* 할인율(당사) */     
                      , MAX(A.WON_ARSE_NSALAMT)               AS WON_ARSE_NSALAMT          /* 증정유발매출(원화) */
                      , MAX(A.ONLN_WON_ARSE_NSALAMT)          AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출(원화) */
                      , MAX(A.ONLN_DLR_ARSE_NSALAMT)          AS ONLN_DLR_ARSE_NSALAMT     /* 온라인유발매출(달러) */
                      , MAX(A.OFLN_WON_ARSE_NSALAMT)          AS OFLN_WON_ARSE_NSALAMT     /* 오프라인유발매출(원화) */
                      , MAX(A.OFLN_DLR_ARSE_NSALAMT)          AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출(달러) */
                      , MAX(A.OMNI_WON_EXCH_NSALAMT)          AS OMNI_WON_EXCH_NSALAMT     /* 교환권옴니매출(원화) */
                      , MAX(A.OMNI_DLR_EXCH_NSALAMT)          AS OMNI_DLR_EXCH_NSALAMT     /* 교환권옴니매출(달러) */
                      , MAX(A.OMNI_WON_NSALAMT)               AS OMNI_WON_NSALAMT          /* 행사옴니매출(원화) */
                      , MAX(A.OMNI_DLR_NSALAMT)               AS OMNI_DLR_NSALAMT          /* 행사옴니매출(달러) */
                      , MAX(A.RGSTPSN_ID)                     AS RGSTPSN_ID                /* 등록자ID */
                      , MAX(A.USR_NM)                         AS USR_NM                    /* 등록자명 */
                      , MAX(A.RGST_DEPTCD)                    AS RGST_DEPTCD               /* 등록부서코드 */
                      , MAX(A.DEPT_NM)                        AS DEPT_NM                   /* 등록부서명 */         
                   FROM (SELECT T.LGPRCD                                                                                                                                                                                    AS LGPRCD
                              , T.LGPROMO_NM                                                                                                                                                                               AS LGPROMO_NM
                              , T.MDPRCD                                                                                                                                                                                   AS MDPRCD
                              , T.MIDPROMO_NM                                                                                                                                                                              AS MIDPROMO_NM
                              , T.PRMTNCD                                                                                                                                                                                  AS PRMTNCD
                              , T.PRMTN_NM                                                                                                                                                                                 AS PRMTN_NM
                              , T.STR_CD                                                                                                                                                                                   AS STR_CD
							  , T.CMPN_OFFER_NO                                                                                                                                                                            AS CMPN_OFFER_NO
                              , T.PRMTN_SECTRG_NO                                                                                                                                                                          AS PRMTN_SECTRG_NO
                              , T.PRMTN_APLY_STRT_AMT                                                                                                                                                                 AS PRMTN_APLY_STRT_AMT
                              , MAX(T.CHNL_TYPE_CD)                                                                                                                                                                        AS CHNL_TYPE_CD                  /* 지점/언어 구분 명(언어) */
                              , MAX(T.PRMTN_LGCSF_CD)                                                                                                                                                                      AS PRMTN_LGCSF_CD
                              , MAX(T.PRMTN_LGCSF_NM)                                                                                                                                                                      AS PRMTN_LGCSF_NM
                              , MAX(T.PRMTN_MDCSF_CD)                                                                                                                                                                      AS PRMTN_MDCSF_CD
                              , MAX(T.PRMTN_MDCSF_NM)                                                                                                                                                                      AS PRMTN_MDCSF_NM
                              , MAX(T.PRMTN_STRT_DT)                                                                                                                                                                       AS PRMTN_STRT_DT
                              , MAX(T.PRMTN_END_DT)                                                                                                                                                                        AS PRMTN_END_DT
                              , SUM(T.WON_EXCH_NSALAMT)                                                                                                                                                                    AS WON_EXCH_NSALAMT
                              , SUM(T.DLR_EXCH_NSALAMT)                                                                                                                                                                    AS DLR_EXCH_NSALAMT
                              , COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0    THEN T.EXCH_SALES_CNT  END)                                                                                  						   AS EXCH_SALES_CNT 	     
                              , ROUND(NVL(SUM(T.WON_EXCH_NSALAMT)   
                              / DECODE(COUNT(DISTINCT CASE WHEN T.WON_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.WON_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END)),0))        AS WON_ARSE_NSALAMT_CUSTRN
                              , ROUND(NVL(SUM(T.DLR_EXCH_NSALAMT)   
                              / DECODE(COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0  THEN T.EXCH_SALES_CNT END)),0))        AS DLR_ARSE_NSALAMT_CUSTRN		      
                              , SUM(T.WON_PRMTN_NSALAMT)                                                                                                                                                                   AS WON_PRMTN_NSALAMT
                              , SUM(T.DLR_PRMTN_NSALAMT)                                                                                                                                                                   AS DLR_PRMTN_NSALAMT
                              , COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0  THEN T.PRMTN_NSALAMT_CNT END)                                                                                						   AS PRMTN_NSALAMT_CNT
                              , ROUND(NVL(SUM(T.WON_PRMTN_NSALAMT)  
                                / DECODE(COUNT(DISTINCT CASE WHEN T.WON_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.WON_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END)),0))  AS WON_PRMTN_NSALAMT_CUSTRN
                              , ROUND(NVL(SUM(T.DLR_PRMTN_NSALAMT)  
                                / DECODE(COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END), 0, NULL, COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0 THEN T.PRMTN_NSALAMT_CNT END)),0))  AS DLR_PRMTN_NSALAMT_CUSTRN
                              , SUM(T.WON_LDFP_ACMLT_AMT)                                                                                                                                                                  AS WON_LDFP_ACMLT_AMT
                              , SUM(T.LDFP_ACMLT_CNT)                                                                                                                                                                      AS LDFP_ACMLT_CNT
                              , COUNT(DISTINCT CASE WHEN T.WON_LDFP_ACMLT_AMT IS NOT NULL AND T.WON_LDFP_ACMLT_AMT <> 0  THEN T.INTG_MEMB_NO END)                                                                          AS LDFP_ACMLT_CUST_NBR
                              , SUM(T.FREELDFP_USE_AMT)                                                                                                                                                                    AS FREELDFP_USE_AMT
                              , SUM(T.FREELDFP_USE_CNT)                                                                                                                                                                    AS FREELDFP_USE_CNT
                              , COUNT(DISTINCT CASE WHEN T.FREELDFP_USE_AMT   IS NOT NULL AND T.FREELDFP_USE_AMT <> 0    THEN T.INTG_MEMB_NO END)                                                                          AS FREELDFP_USE_CUST_NBR
                              , SUM(T.FREELDFP_ACMLT_AMT)                                                                                                                                                                  AS FREELDFP_ACMLT_AMT          /* 프리LDFPAY증정금액(프리LDFPAY적립금액) */
                              , SUM(T.FREELDFP_ACMLT_CNT)                                                                                                                                                                  AS FREELDFP_ACMLT_CNT          /* 프리LDFPAY증정건수(프리LDFPAY적립건수) */ 
                              , COUNT(DISTINCT CASE WHEN T.FREELDFP_ACMLT_AMT IS NOT NULL AND T.FREELDFP_ACMLT_AMT <> 0  THEN T.INTG_MEMB_NO END)                                                                          AS FREELDFP_ACLMLT_CUST_NBR
                              , SUM(T.WON_TOT_DC_AMT)                                                                                                                                                                      AS DC_AMT
                              , SUM(T.DC_CNT)                                                                                                                                                                              AS DC_CNT
                              , COUNT(DISTINCT CASE WHEN T.WON_TOT_DC_AMT     IS NOT NULL AND T.WON_TOT_DC_AMT <> 0      THEN T.INTG_MEMB_NO END)                                                                          AS BDN_CUST_NBR
                              , SUM(T.FGF_PRESTAT_AMT)                                                                                                                                                                     AS FGF_PRESTAT_AMT
                              , SUM(T.FGF_PRESTAT_CNT)                                                                                                                                                                     AS FGF_PRESTAT_CNT
                              , COUNT(DISTINCT CASE WHEN T.FGF_PRESTAT_AMT    IS NOT NULL AND T.FGF_PRESTAT_AMT <> 0     THEN T.INTG_MEMB_NO END)                                                                          AS FGF_PRESTAT_CUST_NBR
                              , MAX(T.ACMLTMN_OFFR_CUST)                                                                                                                                                                   AS ACMLTMN_OFFR_CUST_NBR     /* 적립금제공건수 */
                              , MAX(T.ACMLTMN_OFFR_CNT)                                                                                                                                                                    AS ACMLTMN_OFFR_CNT          /* 적립금증정_건수 */
                              , MAX(T.DLR_ACMLTMN_OFFR_AMT)                                                                                                                                                                AS DLR_ACMLTMN_OFFR_AMT      /* 적립금증정_건수 */ 
                              , SUM(T.ACMLTMN_USE_CNT)                                                                                                                                                                     AS ACMLTMN_USE_CNT
                              , SUM(T.WON_ACMLTMN_PYF_AMT)                                                                                                                                                                 AS WON_ACMLTMN_PYF_AMT
                              , COUNT(DISTINCT CASE WHEN T.WON_ACMLTMN_PYF_AMT IS NOT NULL AND T.WON_ACMLTMN_PYF_AMT <> 0 THEN T.INTG_MEMB_NO END)                                                                         AS ACMLTMN_USE_CUST_NBR
                              , SUM(T.TOT_PRESTAT_AMT)                                                                                                                                                                     AS TOT_PRESTAT_AMT
                              , SUM(T.PRESTAT_OURCOM_BDN_AMT)                                                                                                                                                              AS PRESTAT_OURCOM_BDN_AMT
                              , SUM(T.PRESTAT_ALYCO_BDN_AMT)                                                                                                                                                               AS PRESTAT_ALYCO_BDN_AMT
                              , ROUND(NVL(SUM(T.TOT_PRESTAT_AMT) / DECODE(SUM(T.WON_EXCH_NSALAMT),0,NULL,SUM(T.WON_EXCH_NSALAMT)),0)* 100 ,1)                                                                              AS TOT_PRESTAT_RATE /* 지원율(전체) ADD */
                              , ROUND(NVL(SUM(T.PRESTAT_OURCOM_BDN_AMT) / DECODE(SUM(T.WON_EXCH_NSALAMT),0,NULL,SUM(T.WON_EXCH_NSALAMT)),0) * 100,1)                                                                       AS OURCOM_PRESTAT_RATE /* 지원율(당사) */
                              , SUM(T.WON_TOT_DC_AMT)                                                                                                                                                                      AS TOT_BDN_AMT
                              , SUM(T.DC_OURCOM_BDN_AMT)                                                                                                                                                                   AS DC_OURCOM_BDN_AMT
                              , SUM(T.DC_ALYCO_BDN_AMT)                                                                                                                                                                    AS DC_ALYCO_BDN_AMT
                              , SUM(T.DC_CNT)                                                                                                                                                                              AS TOT_DC_CNT
                              , SUM(T.WON_TOT_DC_AMT)                                                                                                                                                                      AS WON_TOT_DC_AMT
                              , ROUND(NVL(SUM(T.WON_TOT_DC_AMT)     / DECODE(SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT),0,NULL,SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT)),0) * 100 ,1)                          AS DC_TOT_BDN_RATE /* 할인율(전체) ADD */
                              , ROUND(NVL(SUM(T.DC_OURCOM_BDN_AMT)     / DECODE(SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT),0,NULL,SUM(T.WON_EXCH_NSALAMT) + SUM(T.WON_TOT_DC_AMT)),0) * 100 ,1)                        AS DC_OURCOM_BDN_RATE /* 할인율(당사) */
                              , SUM(T.WON_ARSE_NSALAMT)                                                                                                                                                                    AS WON_ARSE_NSALAMT
                              , SUM(T.DLR_ARSE_NSALAMT)                                                                                                                                                                    AS DLR_ARSE_NSALAMT
                              , SUM(T.ONLN_WON_ARSE_NSALAMT)                                                                                                                                                               AS ONLN_WON_ARSE_NSALAMT
                              , SUM(T.ONLN_DLR_ARSE_NSALAMT)                                                                                                                                                               AS ONLN_DLR_ARSE_NSALAMT
                              , SUM(T.OFLN_WON_ARSE_NSALAMT)                                                                                                                                                               AS OFLN_WON_ARSE_NSALAMT
                              , SUM(T.OFLN_DLR_ARSE_NSALAMT)                                                                                                                                                               AS OFLN_DLR_ARSE_NSALAMT
                              , SUM(T.OMNI_WON_EXCH_NSALAMT)                                                                                                                                                               AS OMNI_WON_EXCH_NSALAMT
                              , SUM(T.OMNI_DLR_EXCH_NSALAMT)                                                                                                                                                               AS OMNI_DLR_EXCH_NSALAMT
                              , SUM(T.OMNI_WON_NSALAMT)                                                                                                                                                                    AS OMNI_WON_NSALAMT
                              , SUM(T.OMNI_DLR_NSALAMT)                                                                                                                                                                    AS OMNI_DLR_NSALAMT
                              , MAX(T.RGSTPSN_ID)                                                                                                                                                                          AS RGSTPSN_ID
                              , MAX(T.USR_NM)                                                                                                                                                                              AS USR_NM
                              , MAX(T.RGST_DEPTCD)                                                                                                                                                                         AS RGST_DEPTCD
                              , MAX(T.DEPT_NM)                                                                                                                                                                             AS DEPT_NM
                           FROM (  SELECT A3.LGPRCD                                                                                                           AS LGPRCD     
                                        , A1.LGPROMO_NM                                                                                                       AS LGPROMO_NM 
                                        , A3.MDPRCD                                                                                                           AS MDPRCD     
                                        , A2.MIDPROMO_NM                                                                                                      AS MIDPROMO_NM
                                        , A3.PRMTNCD                                                                                                          AS PRMTNCD    
                                        , A3.PRMTN_NM                                                                                                         AS PRMTN_NM       
                                        , A3.ONOFF_DVS_CD                                                                                                                     AS CHNL_TYPE_CD              /* 채널구분(1:오프라인,2:온라인) */	  
                                        , A3.PRMTN_LGCSF_CD                                                                                                                   AS PRMTN_LGCSF_CD            /* 행사대분류코드 */
                                        , A4.PRMTN_LGCSF_NM                                                                                                                   AS PRMTN_LGCSF_NM            /* 행사대분류코명 */ 
                                        , A3.PRMTN_MDCSF_CD                                                                                                                   AS PRMTN_MDCSF_CD            /* 행사중분류코드 */
                                        , A5.PRMTN_MDCSF_NM                                                                                                                   AS PRMTN_MDCSF_NM            /* 행사중분류명 */
                                        , A8.STR_CD
										, A7.CMPN_OFFER_NO                                                                                                                    AS CMPN_OFFER_NO             /* 행사오퍼번호 */
                                        , A8.PRMTN_SECTRG_NO                                                                                                                  AS PRMTN_SECTRG_NO           /* 구간대코드 */
                                        , A7.PRMTN_APLY_STRT_AMT                                                                                                              AS PRMTN_APLY_STRT_AMT       /* 행사적용시작금액 */ 
                                        , A3.PRMTN_STRT_DT                                                                                                                    AS PRMTN_STRT_DT             /* 행사시작일자 */
                                        , A3.PRMTN_END_DT                                                                                                                     AS PRMTN_END_DT              /* 행사종료일자 */
                                        , A8.INTG_MEMB_NO                                                                                                                     AS INTG_MEMB_NO              /* 통합회원번호 */
                                        , A8.WON_EXCH_NSALAMT                                                                                                                 AS WON_EXCH_NSALAMT          /* 원화교환권순매출액 */
                                        , A8.DLR_EXCH_NSALAMT                                                                                                                 AS DLR_EXCH_NSALAMT          /* 달러교환권순매출액 */
                                        , (CASE WHEN A8.DLR_EXCH_NSALAMT IS NOT NULL THEN A8.INTG_MEMB_NO ELSE NULL END)                   									  AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
                                        , A8.WON_PRMTN_NSALAMT                                                                                                                AS WON_PRMTN_NSALAMT         /* 원화행사순매출액 */
                                        , A8.DLR_PRMTN_NSALAMT                                                                                                                AS DLR_PRMTN_NSALAMT         /* 달러행사순매출액 */
                                        , (CASE WHEN A8.DLR_PRMTN_NSALAMT IS NOT NULL THEN A8.INTG_MEMB_NO ELSE NULL END)                 									  AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
                                        , A8.LDFP_ACMLT_AMT                                                                                                                   AS WON_LDFP_ACMLT_AMT        /* LDFPAY 적립금액 */
                                        , A8.LDFP_ACMLT_CNT                                                                                                                   AS LDFP_ACMLT_CNT            /* LDFPAY 적립건수 */
                                        , A8.FREELDFP_USE_AMT                                                                                                                 AS FREELDFP_USE_AMT          /* 프리LDFPAY사용금액 */
                                        , A8.FREELDFP_USE_CNT                                                                                                                 AS FREELDFP_USE_CNT          /* 프리LDFPAY사용건수 */
                                        , A8.FREELDFP_ACMLT_AMT                                                                                                               AS FREELDFP_ACMLT_AMT        /* 프리LDFPAY증정금액(프리LDFPAY적립금액) */
                                        , A8.FREELDFP_ACMLT_CNT                                                                                                               AS FREELDFP_ACMLT_CNT        /* 프리LDFPAY증정건수(프리LDFPAY적립건수) */ 
                                        , A8.DC_CNT                                                                                                                           AS DC_CNT                    /* 할인건수 */
                                        , A8.FGF_PRESTAT_AMT                                                                                                                  AS FGF_PRESTAT_AMT           /* 사은품증정금액 */
                                        , A8.FGF_PRESTAT_CNT                                                                                                                  AS FGF_PRESTAT_CNT           /* 사은품증정건수 */
                                        , NULL                                                                                                                                AS ACMLTMN_OFFR_CNT          /* 적립금제공건수  */   
                                        , NULL                                                                                                                                AS DLR_ACMLTMN_OFFR_AMT      /* 원화적립금제공금액(달러)  */  
                                        , NULL                                                                                                                                AS ACMLTMN_USE_CNT           /* 적립금사용건수  */  
                                        , NULL                                                                                                                                AS ACMLTMN_OFFR_CUST         /* 적립금증정객수 */
                                        , NULL                                                                                                                                AS WON_ACMLTMN_PYF_AMT       /* 원화적립금결제금액  */ 
                                        , (A8.FGF_PRESTAT_AMT + A8.LDFP_ACMLT_AMT + A8.FREELDFP_USE_AMT)                                                                      AS TOT_PRESTAT_AMT           /* 지원금액(전체)(=사은품증정금액 + LDFPAY적립금액 + 프리LDFPAY사용금액) */
                                        , A8.PRESTAT_OURCOM_BDN_AMT                                                                                                           AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사)(=증정당사부담금액) */
                                        , A8.PRESTAT_ALYCO_BDN_AMT                                                                                                            AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사)(=증정제휴사부담금액) */                     
                                        , A8.DC_OURCOM_BDN_AMT                                                                                                                AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
                                        , A8.DC_ALYCO_BDN_AMT                                                                                                                 AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */  
                                        , A8.WON_PRMTN_DC_AMT                                                                                                                 AS WON_TOT_DC_AMT            /* 원화총할인금액(원화행사할인금액)  */
                                        , A8.WON_ARSE_NSALAMT                                                                                                                 AS WON_ARSE_NSALAMT          /* 증정유발매출(원화유발순매출액) */
                                        , A8.DLR_ARSE_NSALAMT                                                                                                                 AS DLR_ARSE_NSALAMT          /* 달러증정유발매출(달러유발순매출액) */
                                        , A8.WON_CROS_ARSE_NSALAMT                                                                                                            AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출 */
                                        , A8.DLR_CROS_ARSE_NSALAMT                                                                                                            AS ONLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출 */  
                                        , NULL                                                                                                                                AS OFLN_WON_ARSE_NSALAMT     /* 오프라인원화유발순매출액 (=원화교차유발순매출액) */ 
                                        , NULL                                                                                                                                AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인달러유발순매출액 (=달러교차유발순매출액) */       
                                        , A8.OMNI_WON_EXCH_NSALAMT                                                                                                            AS OMNI_WON_EXCH_NSALAMT     /* 옴니원화교환권순매출액 */                     
                                        , A8.OMNI_DLR_EXCH_NSALAMT                                                                                                            AS OMNI_DLR_EXCH_NSALAMT     /* 옴니달러교환권순매출액 */
                                        , A8.OMNI_WON_NSALAMT                                                                                                                 AS OMNI_WON_NSALAMT          /* 옴니교환권순매출액 */       
                                        , A8.OMNI_DLR_NSALAMT                                                                                                                 AS OMNI_DLR_NSALAMT          /* 옴니달러순매출액 */
                                        , A3.RGSTPSN_ID                                                                                                                       AS RGSTPSN_ID                /* 등록자ID */
                                        , A9.USR_NM                                                                                                                           AS USR_NM                    /* 등록자명 */
                                        , A3.RGST_DEPTCD                                                                                                                      AS RGST_DEPTCD               /* 등록부서ID */
                                        , A10.DEPT_NM                                                                                                                         AS DEPT_NM                   /* 등록부서명 */
                                     FROM LDF_DW.D_LGPROMO A1
                                        , LDF_DW.D_MIDPROMO A2
                                        , LDF_DW.D_PRMTN A3
                                        , LDF_DW.D_PRMTN_LGCSF A4 
                                        , LDF_DW.D_PRMTN_MDCSF A5
                                        , LDF_DW.WL_LC_PRMTN_BNFT A6
                                        , LDF_DW.WL_LC_PRMTN_OFFER_SECT A7
                                        , LDF_DW.FL_MK_PRMTN_OFFER_ACTRSLT A8
                                        , LDF_DW.D_USR A9
                                        , LDF_DW.D_DEPT A10                                  
                                    WHERE 1=1
                                      AND A1.LGPRCD = A2.LGPRCD 
                                      AND A2.MDPRCD = A3.MDPRCD 
                                      AND A3.PRMTN_LGCSF_CD = A4.PRMTN_LGCSF_CD (+) 
                                      AND A3.PRMTN_MDCSF_CD = A5.PRMTN_MDCSF_CD (+) 
                                      AND A3.PRMTNCD = A6.PRMTNCD 
                                      AND A6.BNFT_OBJ_NO = A7.CMPN_OFFER_NO (+) 
                                      AND A3.PRMTNCD = A8.PRMTNCD 
                                      AND A7.CMPN_OFFER_NO = A8.OFFER_NO
                                      AND A7.PRMTN_SECTRG_NO = A8.PRMTN_SECTRG_NO
                                      AND A3.RGSTPSN_ID = A9.EMPNO (+) 
                                      AND A3.RGST_DEPTCD = A10.DEPTCD (+)
                                ]]><if test='prmtnDvsCd != null and prmtnDvsCd != "" and prmtncd != null and prmtncd != ""'>
                                   <choose>
                                      <when test='prmtnDvsCd == "1"'> <!-- 대행사 -->
                                      AND A3.LGPRCD         = #{prmtncd}        /* 조회조건 - 행사구분 대행사일 경우 행사코드 */     
                                      </when>
                                      <when test='prmtnDvsCd == "2"'> <!-- 중행사 -->
                                      AND A3.MDPRCD         = #{prmtncd}        /* 조회조건 - 행사구분 중행사일 경우 행사코드 */
                                      </when>
                                      <when test='prmtnDvsCd == "3"'> <!-- 소행사 -->
                                      AND A3.PRMTNCD        = #{prmtncd}       /* 조회조건 - 행사구분 소행사일 경우 행사코드 */
                                      </when>
                                   </choose>
                                   </if>
                                   <if test='inqryStrtDt != null and inqryStrtDt != ""'>
                                      AND A8.STD_DT        >= #{inqryStrtDt}           /* 조회 시작일 */
                                   </if>   
                                   <if test='inqryEndDt != null and inqryEndDt != ""'>
                                      AND A8.STD_DT        <![CDATA[<=]]> #{inqryEndDt}           /* 조회 종료일 */
                                   </if>
                                            /* 조회조건 */
                                 <if test='(strCd != null and strCd != "") or (strCds != null) or (biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != "") or (biznsStrLoctnDvsCds != null)'>         
                                      AND A3.PRMTNCD   IN (SELECT PRMTNCD 
                                                             FROM LDF_DW.WL_LC_PRMTN_OFFR_CNDT A3
                                                                , LDF_DW.D_STR A4
                                                            WHERE 1                          = 1
                                                              AND A3.PRMTN_CNDT_VAL          = A4.STR_CD
                                                              AND PRMTN_OFFR_CNDT_CD         = '01'
                                                              AND CHNL_TYPE_CD               = '02'
                                                   <if test='strCd != null and strCd != ""'>
                                                              AND A3.PRMTN_CNDT_VAL          = #{strCd} /* 조회조건 - 지점 */
                                                   </if>
                                                   <if test='strCds != null'>
                                                              AND A3.PRMTN_CNDT_VAL         IN <foreach collection="strCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 지점 */
                                                   </if>
                                                   <if test='biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != ""'>
                                                              AND A4.BIZNS_STR_LOCTN_DVS_CD  = #{biznsStrLoctnDvsCd} /* 조회조건 - 영업점위치구분 */
                                                   </if>
                                                   <if test='biznsStrLoctnDvsCds != null'>
                                                              AND A4.BIZNS_STR_LOCTN_DVS_CD IN <foreach collection="biznsStrLoctnDvsCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 영업점위치구분 */
                                                   </if>
                                                    )  
                                 </if>
                               UNION ALL
                              SELECT /* FE_MK_혜택OFFER상세 */
                                     A1.LGPRCD                                                 AS LGPRCD
                                   , A2.LGPROMO_NM                                             AS LGPROMO_NM
                                   , A1.MDPRCD                                                 AS MDPRCD
                                   , A3.MIDPROMO_NM                                            AS MIDPROMO_NM
                                   , A1.PRMTNCD                                                AS PRMTNCD 
                                   , A1.PRMTN_NM                                               AS PRMTN_NM
                                   , A1.ONOFF_DVS_CD                                           AS CHNL_TYPE_CD
                                   , A1.PRMTN_LGCSF_CD                                         AS PRMTN_LGCSF_CD
                                   , A4.PRMTN_LGCSF_NM                                         AS PRMTN_LGCSF_NM
                                   , A1.PRMTN_MDCSF_CD                                         AS PRMTN_MDCSF_CD
                                   , A5.PRMTN_MDCSF_NM                                         AS PRMTN_MDCSF_NM
                                   , A1.STR_CD
								   , A9.CMPN_OFFER_NO                                          AS CMPN_OFFER_NO             /* 행사오퍼번호 */
                                   , CAST(A9.PRMTN_SECTRG_NO AS NUMBER(20,0))                   AS PRMTN_SECTRG_NO           /* 구간대코드 */
                                   , A9.PRMTN_APLY_STRT_AMT                                    AS PRMTN_APLY_STRT_AMT       /* 행사적용시작금액 */ 
                                   , A1.PRMTN_STRT_DT                                          AS PRMTN_STRT_DT
                                   , A1.PRMTN_END_DT                                           AS PRMTN_END_DT
                                   , NULL                                                      AS INTG_MEMB_NO              /* 통합회원번호 */
                                   , 0                                                         AS WON_EXCH_NSALAMT          /* 원화교환권순매출액 */
                                   , 0                                                         AS DLR_EXCH_NSALAMT          /* 달러교환권순매출액 */
                                   , NULL                                                      AS EXCH_SALES_CNT            /* 교환권매출_반응객수 */
                                   , 0                                                         AS WON_PRMTN_NSALAMT         /* 원화행사순매출액 */
                                   , 0                                                         AS DLR_PRMTN_NSALAMT         /* 달러행사순매출액 */
                                   , NULL                                                      AS PRMTN_NSALAMT_CNT         /* 행사매출_반응객수 */
                                   , 0                                                         AS WON_LDFP_ACMLT_AMT        /* LDFPAY 적립금액 */
                                   , 0                                                         AS LDFP_ACMLT_CNT            /* LDFPAY 적립건수 */
                                   , 0                                                         AS FREELDFP_USE_AMT          /* 프리LDFPAY사용금액 */
                                   , 0                                                         AS FREELDFP_USE_CNT          /* 프리LDFPAY사용건수 */
                                   , 0                                                         AS FREELDFP_ACMLT_AMT        /* 프리LDFPAY증정금액(프리LDFPAY적립금액) */
                                   , 0                                                         AS FREELDFP_ACMLT_CNT        /* 프리LDFPAY증정건수(프리LDFPAY적립건수) */ 
                                   , 0                                                         AS DC_CNT                    /* 할인건수 */
                                   , 0                                                         AS FGF_PRESTAT_AMT           /* 사은품증정금액 */
                                   , 0                                                         AS FGF_PRESTAT_CNT           /* 사은품증정건수 */
                                   , 0                                                         AS ACMLTMN_OFFR_CNT          /* 적립금제공건수 */
                                   , A1.DLR_ACMLTMN_OFFR_AMT                                   AS DLR_ACMLTMN_OFFR_AMT      /* 적립금제공금액(달러) */
                                   , A1.ACMLTMN_OFFR_CNT                                       AS ACMLTMN_USE_CNT           /* 적립금사용건수 */
                                   , A1.ACMLTMN_OFFR_CUST_NBR                                  AS ACMLTMN_OFFR_CUST         /* 적립금증정객수 */
                                   , 0                                                         AS WON_ACMLTMN_PYF_AMT       /* 원화적립금결제금액  */  
                                   , 0                                                         AS TOT_PRESTAT_AMT           /* 지원금액(전체)(=당사+제휴사) */
                                   , 0                                                         AS PRESTAT_OURCOM_BDN_AMT    /* 지원금액(당사)(=증정당사부담금액) */
                                   , 0                                                         AS PRESTAT_ALYCO_BDN_AMT     /* 지원금액(제휴사)(=증정제휴사부담금액) */                     
                                   , 0                                                         AS DC_OURCOM_BDN_AMT         /* 할인금액(당사) */
                                   , 0                                                         AS DC_ALYCO_BDN_AMT          /* 할인금액(제휴사) */  
                                   , 0                                                         AS WON_TOT_DC_AMT            /* 원화총할인금액  */ 
                                   , 0                                                         AS WON_ARSE_NSALAMT          /* 증정유발매출(원화유발순매출액) */
                                   , 0                                                         AS DLR_ARSE_NSALAMT          /* 달러증정유발매출(달러유발순매출액) */
                                   , 0                                                         AS ONLN_WON_ARSE_NSALAMT     /* 온라인유발매출 */
                                   , 0                                                         AS ONLN_DLR_ARSE_NSALAMT     /* 오프라인유발매출 */  
                                   , 0                                                         AS OFLN_WON_ARSE_NSALAMT     /* 오프라인원화유발순매출액 (=원화교차유발순매출액) */ 
                                   , 0                                                         AS OFLN_DLR_ARSE_NSALAMT     /* 오프라인달러유발순매출액 (=달러교차유발순매출액) */       
                                   , 0                                                         AS OMNI_WON_EXCH_NSALAMT     /* 옴니원화교환권순매출액 */                     
                                   , 0                                                         AS OMNI_DLR_EXCH_NSALAMT     /* 옴니달러교환권순매출액 */
                                   , 0                                                         AS OMNI_WON_NSALAMT          /* 옴니교환권순매출액 */       
                                   , 0                                                         AS OMNI_DLR_NSALAMT          /* 옴니달러순매출액 */
                                   , A1.RGSTPSN_ID                                             AS RGSTPSN_ID                /* 등록자ID */
                                   , A6.USR_NM                                                 AS USR_NM                    /* 등록자명 */
                                   , A1.RGST_DEPTCD                                            AS RGST_DEPTCD               /* 등록부서ID */
                                   , A7.DEPT_NM                                                AS DEPT_NM                   /* 등록부서명 */
                                FROM (SELECT /*+ PARALLEL(4) USE_HASH(B1,B4)  FULL(B1) FULL(B4)*/
                                             B1.PRMTNCD                      AS PRMTNCD 
                                           , B1.PRMTN_NM                     AS PRMTN_NM
                                           , B1.STR_CD                       AS STR_CD
                                           , MAX(B1.LGPRCD)                  AS LGPRCD
                                           , MAX(B1.MDPRCD)                  AS MDPRCD
                                           , MAX(B1.PRMTN_STRT_DT)           AS PRMTN_STRT_DT 
                                           , MAX(B1.PRMTN_END_DT)            AS PRMTN_END_DT 
                                           , MAX(B1.ONOFF_DVS_CD)            AS ONOFF_DVS_CD 
                                           , MAX(B1.PRMTN_LGCSF_CD)          AS PRMTN_LGCSF_CD 
                                           , MAX(B1.PRMTN_MDCSF_CD)          AS PRMTN_MDCSF_CD 
                                           , MAX(B1.RGSTPSN_ID)              AS RGSTPSN_ID 
                                           , MAX(B1.RGST_DEPTCD)             AS RGST_DEPTCD 
                                           , COUNT(DISTINCT B4.INTG_MEMB_NO) AS ACMLTMN_OFFR_CUST_NBR /* 적립금제공건수 */
                                           , SUM(B4.OFFER_PRESTAT_CNT)       AS ACMLTMN_OFFR_CNT /* 적립금증정_건수 */
                                           , SUM(B4.DLR_OFFER_PRESTAT_AMT)   AS DLR_ACMLTMN_OFFR_AMT /* 달러적립금제공금액 */        
                                        FROM (SELECT B3.PRMTNCD
                                                   , B3.PRMTN_NM
                                                   , B3.LGPRCD
                                                   , B3.MDPRCD
                                                   , B3.PRMTN_STRT_DT
                                                   , B3.PRMTN_END_DT
                                                   , B3.ONOFF_DVS_CD
                                                   , B3.PRMTN_LGCSF_CD
                                                   , B3.PRMTN_MDCSF_CD
                                                   , B7.PRMTN_CNDT_VAL       AS STR_CD
                                                   , B3.RGSTPSN_ID
                                                   , B3.RGST_DEPTCD
                                                   , B6.BNFT_OBJ_NO      AS ONLN_OFFER_NO  /* 혜택대상번호 */
                                                   , B6.PRMTN_BNFT_NO    AS PRMTN_BNFT_NO  /* 행사혜택번호 */
                                                FROM LDF_DW.D_PRMTN B3
                                                   , LDF_DW.WL_LC_PRMTN_BNFT B6
                                                   , LDF_DW.WL_LC_PRMTN_OFFR_CNDT B7
                                               WHERE 1=1
                                                 AND B3.PRMTNCD        = B6.PRMTNCD 
	     			  	          	             AND B3.PRMTNCD        = B7.PRMTNCD
                                                 AND B3.PRMTN_LGCSF_CD = '007' /* 적립금 */
                                                 AND B7.PRMTN_OFFR_CNDT_CD   = '01'          /* 지점구분코드 */
                                <if test='prmtnDvsCd != null and prmtnDvsCd != "" and prmtncd != null and prmtncd != ""'>
                                    <choose>
                                         <when test='prmtnDvsCd == "1"'> <!-- 대행사 -->
                                                 AND B3.LGPRCD         = #{prmtncd} /* 조회조건 - 행사구분 대행사일 경우 행사코드 */
                                         </when>
                                         <when test='prmtnDvsCd == "2"'> <!-- 중행사 -->
                                                 AND B3.MDPRCD       = #{prmtncd}       /* 선택조건 - 중행사코드 */
                                         </when>
                                         <when test='prmtnDvsCd == "3"'> <!-- 소행사 -->
                                                 AND B3.PRMTNCD      = #{prmtncd}         /* 조회조건 - 행사구분 소행사일 경우 행사코드 */
                                         </when>
                                    </choose>
                                </if>
                                             ) B1
                                           , LDF_DW.FE_MK_BNFT_OFFER_DTL B4
                                       WHERE 1=1
                                         AND B4.STD_DT       BETWEEN B1.PRMTN_STRT_DT AND B1.PRMTN_END_DT 
                                         AND B4.STD_TM_VAL        >= '00'
                                         AND B4.ONLN_OFFER_NO      = B1.ONLN_OFFER_NO 
                                         AND B4.PRMTN_BNFT_NO      = B1.PRMTN_BNFT_NO 
                                         AND B4.OFFER_PRESTAT_CNT <![CDATA[<>]]> 0 /* 적립건수 0이 아닌경우 */
                                <if test='inqryStrtDt != null and inqryStrtDt != ""'>
                                         AND B4.STD_DT <![CDATA[>=]]> #{inqryStrtDt}  /* 조회 시작일 */
                                </if>
                                <if test='inqryEndDt != null and inqryEndDt != ""'>
                                         AND B4.STD_DT <![CDATA[<=]]> #{inqryEndDt}  /* 조회 종료일 */
                                </if>
                                       GROUP BY B1.PRMTNCD
                                              , B1.PRMTN_NM
                                              , B1.STR_CD    
                                     ) A1
                                     , LDF_DW.D_LGPROMO A2
                                     , LDF_DW.D_MIDPROMO A3
                                     , LDF_DW.D_PRMTN_LGCSF A4
                                     , LDF_DW.D_PRMTN_MDCSF A5
                                     , LDF_DW.D_USR A6
                                     , LDF_DW.D_DEPT A7
                                     , LDF_DW.WL_LC_PRMTN_BNFT A8       /* WL_LC_행사혜택 */
                                     , LDF_DW.WL_LC_PRMTN_OFFER_SECT A9 /* WL_LC_행사OFFER구간 */
                                 WHERE 1=1
                                   AND A1.LGPRCD         = A2.LGPRCD 
                                   AND A1.MDPRCD         = A3.MDPRCD
                                   AND A1.PRMTN_LGCSF_CD = A4.PRMTN_LGCSF_CD (+)
                                   AND A1.PRMTN_MDCSF_CD = A5.PRMTN_MDCSF_CD (+)
                                   AND A1.RGSTPSN_ID     = A6.EMPNO (+)
                                   AND A1.RGST_DEPTCD    = A7.DEPTCD (+)
                                   AND A1.PRMTNCD = A8.PRMTNCD
                                   AND A8.BNFT_OBJ_NO = A9.CMPN_OFFER_NO (+)
                               <if test='(strCd != null and strCd != "") or (strCds != null) or (biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != "") or (biznsStrLoctnDvsCds != null)'>
                                   AND A1.PRMTNCD      IN (SELECT PRMTNCD 
                                                             FROM LDF_DW.WL_LC_PRMTN_OFFR_CNDT A3
                                                                , LDF_DW.D_STR A4
                                                            WHERE 1                          = 1
                                                              AND A3.PRMTN_CNDT_VAL          = A4.STR_CD
                                                              AND A3.PRMTN_OFFR_CNDT_CD      = '01'
                                                              AND A3.CHNL_TYPE_CD            = '02'
                                   <if test='strCd != null and strCd != ""'>
                                                                    AND A3.PRMTN_CNDT_VAL          = #{strCd} /* 조회조건 - 지점 */
                                   </if>
                                   <if test='strCds != null'>
                                                                    AND A3.PRMTN_CNDT_VAL         IN <foreach collection="strCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 지점 */
                                   </if>
                                   <if test='biznsStrLoctnDvsCd != null and biznsStrLoctnDvsCd != ""'>
                                                                    AND A4.BIZNS_STR_LOCTN_DVS_CD  = #{biznsStrLoctnDvsCd} /* 조회조건 - 영업점위치구분 */
                                   </if>
                                   <if test='biznsStrLoctnDvsCds != null'>
                                                                    AND A4.BIZNS_STR_LOCTN_DVS_CD IN <foreach collection="biznsStrLoctnDvsCds" item="item" open="(" close=")" separator=",">#{item}</foreach> /* 조회조건 - 영업점위치구분 */
                                   </if>
                                   )
                               </if>
                                                        
                       ) T   
                 WHERE 1=1
                 GROUP BY T.LGPRCD                                                                                                                                                          
                     , T.LGPROMO_NM                                                                                                                                                     
                     , T.MDPRCD                                                                                                                                                          
                     , T.MIDPROMO_NM                                                                                                                                                    
                     , T.PRMTNCD                                                                                                                                                         
                     , T.PRMTN_NM 
                     , T.STR_CD
                     , T.CMPN_OFFER_NO
                     , T.PRMTN_SECTRG_NO
                     , T.PRMTN_APLY_STRT_AMT 
           ) A
       GROUP BY A.LGPRCD                            
             , A.LGPROMO_NM                       
             , A.MDPRCD                           
             , A.MIDPROMO_NM                  
             , A.PRMTNCD                          
             , A.PRMTN_NM
             , A.STR_CD
             , A.PRMTN_APLY_STRT_AMT
        ) M
