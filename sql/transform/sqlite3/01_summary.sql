-- ============================================================
-- TRANSFORM: SQLite3 target용 집계 테이블 생성
-- TB_CONTRACT + TB_PAYMENT → 월별/상품별 집계
-- ============================================================

DROP TABLE IF EXISTS TB_CONTRACT_SUMMARY;

CREATE TABLE TB_CONTRACT_SUMMARY AS
SELECT
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS,
    COUNT(c.CONTRACT_ID)                    AS CONTRACT_CNT,
    SUM(c.CONTRACT_AMT)                     AS CONTRACT_AMT_SUM,
    ROUND(AVG(c.CONTRACT_AMT), 0)           AS CONTRACT_AMT_AVG,
    COALESCE(SUM(p.PAY_AMT), 0)            AS PAY_AMT_SUM,
    COUNT(p.PAYMENT_ID)                     AS PAY_CNT
FROM TB_CONTRACT c
LEFT JOIN TB_PAYMENT p
    ON c.CONTRACT_ID = p.CONTRACT_ID
   AND c.CLS_YYMM    = p.CLS_YYMM
GROUP BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
ORDER BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
