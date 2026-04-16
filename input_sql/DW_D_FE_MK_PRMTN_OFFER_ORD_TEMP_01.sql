/**********  UQ_CREATE_01  ****************************************************/


COMMIT;




/**********  UQ_DELETE_01  ****************************************************/


EXECUTE IMMEDIATE 'TRUNCATE TABLE FE_MK_PRMTN_OFFER_ORD_TEMP';





/**********  UQ_INSERT_01  ****************************************************/


EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';


INSERT /*+ APPEND PARALLEL(4) */
  INTO FE_MK_PRMTN_OFFER_ORD_TEMP
SELECT /*+ PARALLEL(4) */
       STD_DT
     , ONLN_ORD_NO
     , PRMTNCD
     , LANG_CD
     , DVIC_CD
     , INTG_MEMB_NO
     , MAX(ONLN_MEMB_NO) AS ONLN_MEMB_NO
     , NVL(MAX(ONLN_OFFER_NO), 0)
     , NVL(MAX(PRMTN_SECTRG_NO), 0)
     , SYSDATE           AS LOAD_DTTM
  FROM
     (
       /* 주문쿠폰사용 */
       SELECT /*+ USE_HASH(T0 T1) PARALLEL(4) */
              T5.ORD_DT                        AS STD_DT
             /* TO_CHAR(T1.USE_DTIME,'YYYYMMDD') AS STD_DT */
            , T2.PRMTNCD                       AS PRMTNCD
            , NVL(T5.LANG_CD   ,'z')           AS LANG_CD
            , NVL(T5.DVC_CD    ,'z')           AS DVIC_CD
            , T1.ORD_NO                        AS ONLN_ORD_NO
            , T1.MBR_NO                        AS ONLN_MEMB_NO
            , T6.INTG_MEMB_NO
            , TO_NUMBER(T4.ERP_EVT_OFR_CD)     AS ONLN_OFFER_NO
            , TO_NUMBER(T4.EVT_RNG_NO)         AS PRMTN_SECTRG_NO
         FROM WE_MT_ISSU_CPN  T1 /* W_발행쿠폰 */
        INNER JOIN WE_MT_EVT_FVR T3 /* WE_행사혜택 */
           ON T1.EVT_FVR_NO  = T3.EVT_FVR_NO
        INNER JOIN D_PRMTN T2       /* D_행사 */
           ON CAST(T3.EVT_NO AS VARCHAR(10)) = T2.PRMTNCD
          AND TO_CHAR(T1.USE_DTIME,'YYYYMMDD') BETWEEN T2.PRMTN_STRT_DT AND T2.PRMTN_END_DT
        INNER JOIN WE_OD_ORD_DSCNT T4  /* WE_주문할인 */
           ON T1.ISSU_CPN_NO = T4.ISSU_CPN_NO
        INNER JOIN WE_OD_ORD T5        /* WE_주문 */
           ON T4.ORD_NO      = T5.ORD_NO
         /* 변경적재 모수 ord_no조인 */
        INNER JOIN FE_MK_ORD_TEMP T0
           ON T0.ONLN_ORD_NO = T5.ORD_NO
         ---------------------------------------
         LEFT OUTER JOIN DE_ONLN_MEMB T6  /* DE_온라인회원 */
           ON T1.MBR_NO = T6.ONLN_MEMB_NO
        WHERE T1.OFR_OCCUR_STAT_CD  = '01'  /* 01 발생 02 지급취소 03 발행취소 */
          AND T1.USE_YN             = 'Y'
          AND T4.ORD_DSCNT_KND_CD   = '02'  /* 01 세일 02 쿠폰 03 할인 */
          AND T4.ORD_DSCNT_STAT_CD IN ('01','02')  /* 주문할인상태코드 01:할인정상 02:할인취소 */
        UNION ALL
       /* 주문할인사용 */
       SELECT /*+ USE_HASH(T0 T1) PARALLEL(4) */
              T2.ORD_DT              AS STD_DT
            , T4.PRMTNCD             AS PRMTNCD
            , NVL(T2.LANG_CD   ,'z') AS LANG_CD
            , NVL(T2.DVC_CD    ,'z') AS DVIC_CD
            , T1.ORD_NO              AS ONLN_ORD_NO
            , T2.MBR_NO              AS ONLN_MEMB_NO
            , T6.INTG_MEMB_NO        AS INTG_MEMB_NO
            , TO_NUMBER(T1.ERP_EVT_OFR_CD) 
            , TO_NUMBER(T1.EVT_RNG_NO) 
         FROM WE_OD_ORD_DSCNT  T1   /* WE_주문할인 */
        INNER JOIN WE_OD_ORD   T2   /* WE_주문 */
           ON T1.ORD_NO            = T2.ORD_NO
          AND T1.ORD_DSCNT_KND_CD IN ('01','03') /* 01 세일 02 쿠폰 03 할인 */
        INNER JOIN WE_MT_EVT_FVR T3  /* WE_행사혜택 */
           ON T1.ORD_DSCNT_REF_NO  = T3.EVT_FVR_NO
        INNER JOIN D_PRMTN T4   /* D_행사 */
           ON CAST(T3.EVT_NO AS VARCHAR(10)) = T4.PRMTNCD
          AND T2.ORD_DT BETWEEN T4.PRMTN_STRT_DT AND T4.PRMTN_END_DT
         /*-- 변경적재 모수 ord_no조인----------------*/
        INNER JOIN FE_MK_ORD_TEMP T0
           ON T0.ONLN_ORD_NO       = T2.ORD_NO
         ---------------------------------------
         LEFT OUTER JOIN DE_ONLN_MEMB T6  /* DE_온라인회원 */
           ON T2.MBR_NO            = T6.ONLN_MEMB_NO
        WHERE T1.ORD_DSCNT_STAT_CD IN ('01','02')   /* 주문할인상태코드 01:할인정상 02:할인취소 */
        UNION ALL
       /* 딜행사 추가 20231205 */		
       SELECT /*+ USE_HASH(T0 T1) PARALLEL(4) */
              T2.ORD_DT              AS STD_DT
            , T4.PRMTNCD             AS PRMTNCD
            , NVL(T2.LANG_CD   ,'z') AS LANG_CD
            , NVL(T2.DVC_CD    ,'z') AS DVIC_CD
            , T1.ORD_NO              AS ONLN_ORD_NO
            , T2.MBR_NO              AS ONLN_MEMB_NO
            , T6.INTG_MEMB_NO        AS INTG_MEMB_NO
            , TO_NUMBER(T1.ERP_EVT_OFR_CD) 
            , TO_NUMBER(T1.EVT_RNG_NO) 
         FROM WE_OD_ORD_DSCNT  T1   /* WE_주문할인 */
        INNER JOIN WE_OD_ORD   T2   /* WE_주문 */
           ON T1.ORD_NO            = T2.ORD_NO
          AND T1.ORD_DSCNT_KND_CD IN ('01','03') /* 01 세일 02 쿠폰 03 할인 */
        INNER JOIN WE_MT_DEAL_EVT_FVR T2  /* WE_딜행사혜택 */
           ON T1.ORD_DSCNT_REF_NO  = T2.DEAL_EVT_FVR_NO
        INNER JOIN WE_MT_DEAL_EVT     T3  /* WE_딜행사 */
           ON T2.DEAL_EVT_NO = T3.DEAL_EVT_NO
        INNER JOIN  D_PRMTN T4   /* D_행사 */
          ON CAST(T3.EVT_NO AS VARCHAR(10)) = T4.PRMTNCD
          AND T2.ORD_DT BETWEEN T4.PRMTN_STRT_DT AND T4.PRMTN_END_DT
         /*-- 변경적재 모수 ord_no조인----------------*/
        INNER JOIN FE_MK_ORD_TEMP T0
           ON T0.ONLN_ORD_NO       = T2.ORD_NO
         ---------------------------------------
         LEFT OUTER JOIN DE_ONLN_MEMB T6  /* DE_온라인회원 */
           ON T2.MBR_NO            = T6.ONLN_MEMB_NO
        WHERE T1.ORD_DSCNT_STAT_CD IN ('01','02')   /* 주문할인상태코드 01:할인정상 02:할인취소 */		
        UNION ALL
       /* 적립금 사용-유발포함 */
       SELECT /*+ USE_HASH(T0 T1 T3) PARALLEL(4) */
              T5.ORD_DT                          AS STD_DT
             /* TO_CHAR(T1.OCCUR_DTIME,'YYYYMMDD') AS STD_DT */
            , T4.PRMTNCD                         AS PRMTNCD
            , NVL(T5.LANG_CD   ,'z')             AS LANG_CD
            , NVL(T5.DVC_CD    ,'z')             AS DVIC_CD
            , NVL(T1.ORD_NO,'-999999999')        AS ONLN_ORD_NO
            , T1.MBR_NO                          AS ONLN_MEMB_NO
            , T6.INTG_MEMB_NO                    AS INTG_MEMB_NO
            , T1.OFR_NO 
            , T3.EVT_SUP_RNG_NO 
         FROM WE_MB_SVMN_RSRV_USE T1  /* W_적립금적립사용 */
        INNER JOIN WE_MT_EVT_FVR  T3  /* WE_행사혜택 */
           ON T1.EVT_FVR_NO = T3.EVT_FVR_NO
        INNER JOIN D_PRMTN T4  /* D_행사 */
           ON CAST(T3.EVT_NO AS VARCHAR(10)) = T4.PRMTNCD
          AND TO_CHAR(T1.OCCUR_DTIME,'YYYYMMDD') BETWEEN T4.PRMTN_STRT_DT AND (CASE WHEN T4.PRMTN_END_DT < '99991201' THEN TO_CHAR(TO_DATE(T4.PRMTN_END_DT,'YYYYMMDD') + 30,'YYYYMMDD') ELSE T4.PRMTN_END_DT END)
         INNER JOIN WE_OD_ORD T5      /* WE_주문 */
           ON T1.ORD_NO = T5.ORD_NO
         /*-- 변경적재 모수 ord_no조인----------------*/
        INNER JOIN FE_MK_ORD_TEMP T0
           ON T0.ONLN_ORD_NO = T1.ORD_NO
         ---------------------------------------
         LEFT OUTER JOIN DE_ONLN_MEMB T6  /* DE_온라인회원 */
           ON T1.MBR_NO = T6.ONLN_MEMB_NO
        WHERE T1.RSRV_USE_SCT_CD IN ('03','04') /* 03 사용  04 사용취소 */
        UNION ALL
       /* LDFPAY 비구매적립 */
       SELECT /*+ PARALLEL(4) */
              T0.LDFP_HIST_HAPN_DT               AS STD_DT
            , CAST(T2.EVT_NO AS VARCHAR(10))     AS PRMTNCD
            , 'KO'                               AS LANG_CD
            , 'z'                                AS DVIC_CD
            , NVL(T0.ONLN_ORD_NO, '-999999999')  AS ONLN_ORD_NO
            , T3.ONLN_MEMB_NO                    AS ONLN_MEMB_NO
            , T0.INTG_MEMB_NO                    AS INTG_MEMB_NO
            , T0.CMPN_OFFER_NO
            , TO_NUMBER(T0.PRMTN_SECTRG_NO)
         FROM FL_MK_LDFP_ACMLT_USE_PTCLS  T0 /* FL_MK_LDFPAY적립사용내역 */
        INNER JOIN D_LDFP T1   /* D_LDFPAY */
           ON T0.LDFP_NO             = T1.LDFP_NO
          AND T1.LDFP_PBLSH_DVS_CD  IN ('1','3')  /* LDF페이발행구분코드 1:온라인(정액) 3:온라인(정률) */
        INNER JOIN WE_MT_EVT_FVR T2  /* WE_행사혜택 */
          ON T0.PRMTNCD              = CAST(T2.LRWD_EVT_NO AS VARCHAR(10))  /* 온라인LDFP관련 행사코드는 LRWD_EVT_NO로 행사혜택의 EVT_NO를 찾아야 함.*/
        INNER JOIN WE_MT_OFR T4  /* WE_오퍼 */
          ON T2.FVR_TGT_NO = T4.OFR_NO
        INNER JOIN WE_MT_EVT T5  /* WE_행사 */
           ON T2.EVT_NO             = T5.EVT_NO
          AND T0.LDFP_HIST_HAPN_DT BETWEEN TO_CHAR(T5.EVT_STRT_DTIME,'YYYYMMDD') AND TO_CHAR(T5.EVT_END_DTIME,'YYYYMMDD')
        INNER JOIN D_INTG_MEMB T3 /* D_통합회원 */
           ON T0.INTG_MEMB_NO     = T3.INTG_MEMB_NO
        WHERE T0.LDFP_ACTI_YN     = 'Y'
          AND T0.LDFP_NO_STAT_CD <> '00'
          AND T0.LDFP_RCPDSBS_TYPE_CD  IN ('11','24')    /* LDF페이수불유형코드 11:지급 24:취소(적립) */
          AND T0.PRMTNCD NOT IN ('11000002', '31000001') /* CS보상 ,정률기본적립 */
          AND T0.LDFP_HIST_HAPN_DT BETWEEN '$$[DW_STRT_DT]' AND '$$[DW_END_DT]'
        UNION ALL
       /* LDFPAY 유발매출 */
       SELECT /*+ USE_HASH(T3 T0) PARALLEL(4) */
              T0.LDFP_HIST_HAPN_DT               AS STD_DT
            , CAST(T2.EVT_NO AS VARCHAR(10))     AS PRMTNCD
            , 'KO'                               AS LANG_CD
            , 'z'                                AS DVIC_CD
            , NVL(T0.ONLN_ORD_NO, '-999999999')  AS ONLN_ORD_NO
            , T6.ONLN_MEMB_NO                    AS ONLN_MEMB_NO
            , T0.INTG_MEMB_NO                    AS INTG_MEMB_NO
            , T0.CMPN_OFFER_NO
            , TO_NUMBER(T0.PRMTN_SECTRG_NO)
         FROM FL_MK_LDFP_ACMLT_USE_PTCLS  T0 /* FL_MK_LDFPAY적립사용내역 */
         /*-- 변경적재 모수 ord_no조인----------------*/
        INNER JOIN FE_MK_ORD_TEMP T3
           ON T3.ONLN_ORD_NO = T0.ONLN_ORD_NO
         ---------------------------------------
        INNER JOIN D_LDFP T1  /* D_LDFPAY */
           ON T0.LDFP_NO = T1.LDFP_NO
          AND T1.LDFP_PBLSH_DVS_CD  IN ('1','3')  /* LDF페이발행구분코드 1:온라인(정액) 3:온라인(정률) */
        INNER JOIN WE_MT_EVT_FVR T2  /* WE_행사혜택 */
           ON T0.PRMTNCD    = CAST(T2.LRWD_EVT_NO AS VARCHAR(10))  /* 온라인LDFP관련 행사코드는 LRWD_EVT_NO로 행사혜택의 EVT_NO를 찾아야 함.*/
        INNER JOIN WE_MT_OFR T4  /* WE_오퍼 */
           ON T2.FVR_TGT_NO = T4.OFR_NO
        INNER JOIN WE_MT_EVT T5  /* WE_행사 */
           ON T2.EVT_NO     = T5.EVT_NO
          AND T0.LDFP_HIST_HAPN_DT BETWEEN TO_CHAR(T5.EVT_STRT_DTIME,'YYYYMMDD')
                                   AND (CASE WHEN TO_CHAR(T5.EVT_END_DTIME,'YYYYMMDD') < '99991201' THEN TO_CHAR( T5.EVT_END_DTIME + 30,'YYYYMMDD') ELSE TO_CHAR(T5.EVT_END_DTIME,'YYYYMMDD') END)
        INNER JOIN D_INTG_MEMB T6 /* D_통합회원 */
           ON T0.INTG_MEMB_NO          = T6.INTG_MEMB_NO
        WHERE T0.LDFP_ACTI_YN          = 'Y'
          AND T0.LDFP_NO_STAT_CD      <> '00'
          AND T0.LDFP_RCPDSBS_TYPE_CD IN ('21','26') /* LDF페이수불유형코드 21:차감 26:취소(사용) */
          AND T0.PRMTNCD          NOT IN ('11000002', '31000001') /* CS보상 , 정률기본적립 */
      ) T1
 GROUP BY STD_DT
        , ONLN_ORD_NO
        , PRMTNCD
        , LANG_CD
        , DVIC_CD
        , INTG_MEMB_NO
        , ONLN_OFFER_NO
        , PRMTN_SECTRG_NO
;

O_COUNT := SQL%ROWCOUNT;
COMMIT;





/**********  UQ_ANALYZE_01  ***************************************************/


DBMS_STATS.GATHER_TABLE_STATS('LDF_DW', 'FE_MK_PRMTN_OFFER_ORD_TEMP', CASCADE=>TRUE, METHOD_OPT=>'FOR ALL INDEXED COLUMNS', DEGREE=>4);