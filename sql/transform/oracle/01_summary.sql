-- ============================================================
-- TRANSFORM: Oracle target용 집계 테이블 생성 (PL/SQL 없음)
-- ============================================================

-- 집계 테이블 생성 (DROP은 00_cleanup.sql에서 처리)
CREATE TABLE @{schema}TB_CONTRACT_SUMMARY2 AS
SELECT
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS,
    COUNT(c.CONTRACT_ID)            AS CONTRACT_CNT,
    SUM(c.CONTRACT_AMT)             AS CONTRACT_AMT_SUM,
    ROUND(AVG(c.CONTRACT_AMT), 0)   AS CONTRACT_AMT_AVG,
    COALESCE(SUM(p.PAYMENT_AMT), 0) AS PAY_AMT_SUM,
    COUNT(p.PAYMENT_ID)             AS PAY_CNT
FROM @{schema}TB_CONTRACT c
LEFT JOIN @{schema}TB_PAYMENT p
    ON c.CONTRACT_ID = p.CONTRACT_ID
GROUP BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
ORDER BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
