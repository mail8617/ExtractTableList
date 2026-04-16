/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

DELETE /*+ PARALLEL(4) */
  FROM SO_LC_LGPROMO_ACTRSLT_AGRG TG
 WHERE EXISTS (SELECT 1
                 FROM D_MIDPROMO EX
                WHERE TG.LGPRCD = EX.LGPRCD
                  AND TRUNC(SYSDATE-1) BETWEEN MIDPROMO_STRT_DTTM AND CASE WHEN MIDPROMO_END_DTTM >= TO_DATE('99990101', 'YYYYMMDD') THEN MIDPROMO_END_DTTM
                                                                           ELSE MIDPROMO_END_DTTM+30 END) /* 진행중인 중행사가 있는 대행사만 집계 */
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

INSERT /*+ APPEND */
  INTO SO_LC_LGPROMO_ACTRSLT_AGRG
SELECT T.LGPRCD                                                                                                                                       /* 대행사코드 */
     , MAX(T.LGPROMO_STRT_DT)                                                                                               AS LGPROMO_STRT_DT        /* 대행사시작일자 */
     , MAX(T.LGPROMO_END_DT)                                                                                                AS LGPROMO_END_DT         /* 대행사종료일자 */
     , SUM(T.DLR_EXCH_NSALAMT)                                                                                              AS DLR_EXCH_NSALAMT       /* 달러교환권순매출액 */
     , SUM(T.WON_EXCH_NSALAMT)                                                                                              AS WON_EXCH_NSALAMT       /* 원화교환권순매출액 */
     , COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0 THEN T.INTG_MEMB_NO END)                                            AS EXCH_RCTN_PSN_NBR      /* 교환권반응자수 */
   --, COUNT(DISTINCT CASE WHEN T.DLR_EXCH_NSALAMT <> 0 OR T.WON_EXCH_NSALAMT <> 0 THEN T.INTG_MEMB_NO END)   AS EXCH_RCTN_PSN_NBR      /* 교환권반응자수 */
     , SUM(T.DLR_PRMTN_NSALAMT)                                                                                             AS DLR_PRMTN_NSALAMT      /* 달러행사순매출액 */
     , SUM(T.WON_PRMTN_NSALAMT)                                                                                             AS WON_PRMTN_NSALAMT      /* 원화행사순매출액 */
     , COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0 THEN T.INTG_MEMB_NO ELSE NULL END)                                 AS PRMTN_RCTN_PSN_NBR     /* 행사반응자수 */
   --, COUNT(DISTINCT CASE WHEN T.DLR_PRMTN_NSALAMT <> 0 OR T.WON_PRMTN_NSALAMT <> 0 THEN T.INTG_MEMB_NO END) AS PRMTN_RCTN_PSN_NBR     /* 행사반응자수 */
     , SUM(T.SUPT_AMT)                                                                                                      AS SUPT_AMT               /* 지원금액 */
     , SUM(T.OURCOM_BDN_SUPT_AMT)                                                                                           AS OURCOM_BDN_SUPT_AMT    /* 당사부담지원금액 */
     , SUM(T.ALYCO_BDN_SUPT_AMT)                                                                                            AS ALYCO_BDN_SUPT_AMT     /* 제휴사부담지원금액 */
     , SUM(T.WON_TOT_DC_AMT)                                                                                                AS WON_TOT_DC_AMT         /* 원화총할인금액 */
     , SUM(T.DC_OURCOM_BDN_AMT)                                                                                             AS DC_OURCOM_BDN_AMT      /* 할인당사부담금액 */
     , SUM(T.DC_ALYCO_BDN_AMT)                                                                                              AS DC_ALYCO_BDN_AMT       /* 할인제휴사부담금액 */
     , SUM(T.DLR_ARSE_NSALAMT)                                                                                              AS DLR_ARSE_NSALAMT       /* 달러유발순매출액 */
     , SUM(T.WON_ARSE_NSALAMT)                                                                                              AS WON_ARSE_NSALAMT       /* 원화유발순매출액 */
     , SUM(T.ONLN_DLR_ARSE_NSALAMT)                                                                                         AS ONLN_DLR_ARSE_NSALAMT  /* 온라인달러유발순매출액 */
     , SUM(T.ONLN_WON_ARSE_NSALAMT)                                                                                         AS ONLN_WON_ARSE_NSALAMT  /* 온라인원화유발순매출액 */
     , SUM(T.OFLN_DLR_ARSE_NSALAMT)                                                                                         AS OFLN_DLR_ARSE_NSALAMT  /* 오프라인달러유발순매출액 */
     , SUM(T.OFLN_WON_ARSE_NSALAMT)                                                                                         AS OFLN_WON_ARSE_NSALAMT  /* 오프라인원화유발순매출액 */
     , SUM(T.OMNI_DLR_EXCH_NSALAMT)                                                                                         AS OMNI_DLR_EXCH_NSALAMT  /* 옴니달러교환권순매출액 */
     , SUM(T.OMNI_WON_EXCH_NSALAMT)                                                                                         AS OMNI_WON_EXCH_NSALAMT  /* 옴니원화교환권순매출액 */
     , SUM(T.OMNI_DLR_NSALAMT)                                                                                              AS OMNI_DLR_NSALAMT       /* 옴니달러순매출액 */
     , SUM(T.OMNI_WON_NSALAMT)                                                                                              AS OMNI_WON_NSALAMT       /* 옴니원화순매출액 */
     , SYSDATE     AS LOAD_DTTM
  FROM (SELECT A1.LGPRCD                                         /* 대행사코드 */
             , A2.LGPROMO_STRT_DT                                /* 행사시작일자 */
             , A2.LGPROMO_END_DT                                 /* 행사종료일자 */
             , A3.INTG_MEMB_NO                                   /* 통합고객번호 */
             , A3.DLR_EXCH_NSALAMT                               /* 달러교환권순매출 */
             , A3.WON_EXCH_NSALAMT                               /* 원화교환권순매출 */
             , A3.DLR_PRMTN_NSALAMT                              /* 달러행사순매출 */
             , A3.WON_PRMTN_NSALAMT                              /* 원화행사순매출 */
             , A3.FGF_PRESTAT_AMT + A3.LDFP_ACMLT_AMT + A3.FREELDFP_USE_AMT             AS SUPT_AMT /* 지원금액 */
             , A3.FGF_PRESTAT_AMT + A3.LDFP_OURCOM_BDN_AMT + A3.FREELDFP_OURCOM_BDN_AMT AS OURCOM_BDN_SUPT_AMT /* 당사부담지원금액 */
             , A3.LDFP_ALYCO_BDN_AMT + A3.FREELDFP_ALYCO_BDN_AMT                        AS ALYCO_BDN_SUPT_AMT  /* 제휴사부담지원금액 */
             , A3.WON_PRMTN_DC_AMT      AS WON_TOT_DC_AMT        /* 원화총할인금액 */
             , A3.DC_OURCOM_BDN_AMT                              /* 할인당사부담금액 */
             , A3.DC_ALYCO_BDN_AMT                               /* 할인제휴사부담금액 */
             , A3.DLR_ARSE_NSALAMT                               /* 달러유발순매출액 */
             , A3.WON_ARSE_NSALAMT                               /* 원화유발순매출액 */
             , NULL                     AS ONLN_DLR_ARSE_NSALAMT /* 온라인달러유발순매출액 */
             , NULL                     AS ONLN_WON_ARSE_NSALAMT /* 온라인원화유발순매출액 */
             , A3.DLR_ARSE_NSALAMT      AS OFLN_DLR_ARSE_NSALAMT /* 오프라인달러유발순매출액 */
             , A3.WON_ARSE_NSALAMT      AS OFLN_WON_ARSE_NSALAMT /* 오프라인달러유발순매출액 */
             , A3.OMNI_DLR_EXCH_NSALAMT                          /* 옴니달러교환권순매출액 */
             , A3.OMNI_WON_EXCH_NSALAMT                          /* 옴니원화교환권순매출액 */
             , A3.OMNI_DLR_NSALAMT                               /* 옴니달러순매출액 */
             , A3.OMNI_WON_NSALAMT                               /* 옴니교환권순매출액 */
          FROM D_LGPROMO  A1
             , (SELECT A21.LGPRCD
                     , TO_CHAR(MIN(A21.MIDPROMO_STRT_DTTM), 'YYYYMMDD') AS LGPROMO_STRT_DT
                     , TO_CHAR(MAX(A21.MIDPROMO_END_DTTM), 'YYYYMMDD')  AS LGPROMO_END_DT
                  FROM D_MIDPROMO A21
                 GROUP BY A21.LGPRCD
               ) A2
             , FL_MK_LGPROMO_ACTRSLT A3 /* 오프라인대행사실적 */
         WHERE A1.LGPRCD = A2.LGPRCD
           AND A2.LGPRCD = A3.LGPRCD
           AND EXISTS (SELECT 1
                         FROM D_MIDPROMO EX
                        WHERE A1.LGPRCD = EX.LGPRCD
                          AND TRUNC(SYSDATE-1) BETWEEN EX.MIDPROMO_STRT_DTTM AND CASE WHEN EX.MIDPROMO_END_DTTM >= TO_DATE('99990101', 'YYYYMMDD') THEN EX.MIDPROMO_END_DTTM
                                                                                      ELSE EX.MIDPROMO_END_DTTM+30 
                                                                                  END) /* 진행중인 중행사가 있는 대행사만 집계 */
         UNION ALL
        SELECT A1.LGPRCD                                                                      /* 대행사코드 */
             , A2.LGPROMO_STRT_DT                                                             /* 행사시작일자 */
             , A2.LGPROMO_END_DT                                                              /* 행사종료일자 */
             , A3.INTG_MEMB_NO                                                                /* 통합고객번호 */
             , A3.DLR_NSALAMT                                                                 /* 달러교환권순매출 */
             , A3.WON_NSALAMT                                                                 /* 원화교환권순매출 */
             , A3.DLR_PRMTN_NSALAMT                                                           /* 달러행사순매출 */
             , A3.WON_PRMTN_NSALAMT                                                           /* 원화행사순매출 */
             , A3.WON_ACMLTMN_PYF_AMT + A3.WON_LDFP_ACMLT_AMT       AS SUPT_AMT               /* 지원금액 */
             , A3.ACMLTMN_OURCOM_BDN_AMT + A3.LDFP_OURCOM_BDN_AMT   AS OURCOM_BDN_SUPT_AMT    /* 당사부담지원금액 */
             , A3.ACMLTMN_ALYCO_BDN_AMT + A3.LDFP_ALYCO_BDN_AMT     AS ALYCO_BDN_SUPT_AMT     /* 제휴사부담지원금액 */
             , A3.WON_PRMTN_DC_AMT                                  AS WON_TOT_DC_AMT         /* 원화총할인금액 */
             , A3.DC_OURCOM_BDN_AMT                                                           /* 할인당사부담금액 */
             , A3.DC_ALYCO_BDN_AMT                                                            /* 할인제휴사부담금액 */
             , A3.DLR_ARSE_NSALAMT                                                            /* 달러유발매출 */
             , A3.WON_ARSE_NSALAMT                                                            /* 원화유발매출 */
             , A3.DLR_ARSE_NSALAMT                                  AS ONLN_DLR_ARSE_NSALAMT  /* 온라인달러유발매출 */
             , A3.WON_ARSE_NSALAMT                                  AS ONLN_WON_ARSE_NSALAMT  /* 온라인원화유발매출 */
             , NULL                                                 AS OFLN_DLR_ARSE_NSALAMT  /* 오프라인달러유발매출 */
             , NULL                                                 AS OFLN_WON_ARSE_NSALAMT  /* 오프라인원화유발매출 */
             , A3.OMNI_DLR_EXCH_NSALAMT                                                       /* 옴니달러교환권순매출 */
             , A3.OMNI_WON_EXCH_NSALAMT                                                       /* 옴니원화교환권순매출 */
             , A3.OMNI_DLR_NSALAMT                                                            /* 옴니달러순매출 */
             , A3.OMNI_WON_NSALAMT                                                            /* 옴니교환권순매출 */
          FROM D_LGPROMO  A1
             , (SELECT A21.LGPRCD
                     , TO_CHAR(MIN(A21.MIDPROMO_STRT_DTTM), 'YYYYMMDD') AS LGPROMO_STRT_DT
                     , TO_CHAR(MAX(A21.MIDPROMO_END_DTTM), 'YYYYMMDD')  AS LGPROMO_END_DT
                  FROM D_MIDPROMO A21
                 GROUP BY A21.LGPRCD
               ) A2
             , FE_MK_LGPROMO_ACTRSLT A3 /* 온라인대행사실적 */
         WHERE A1.LGPRCD = A2.LGPRCD
           AND A2.LGPRCD = A3.LGPRCD
           AND EXISTS (SELECT 1
                         FROM D_MIDPROMO EX
                        WHERE A1.LGPRCD = EX.LGPRCD
                          AND TRUNC(SYSDATE-1) BETWEEN EX.MIDPROMO_STRT_DTTM AND CASE WHEN EX.MIDPROMO_END_DTTM >= TO_DATE('99990101', 'YYYYMMDD') THEN EX.MIDPROMO_END_DTTM
                                                                                      ELSE EX.MIDPROMO_END_DTTM+30 END) /* 진행중인 중행사가 있는 대행사만 집계 */
       ) T
 GROUP BY T.LGPRCD;


O_COUNT := SQL%ROWCOUNT;
COMMIT;