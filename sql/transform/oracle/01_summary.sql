-- ============================================================
-- TRANSFORM: Oracle target용 집계 테이블 생성 (PL/SQL 없음)
-- ============================================================

-- 세션 스키마(GUI schema 필드)에서 실행됨 — 접두사 불필요
CREATE TABLE TB_CONTRACT_SUMMARY2 AS
SELECT
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS,
    COUNT(c.CONTRACT_ID)            AS CONTRACT_CNT,
    SUM(c.CONTRACT_AMT)             AS CONTRACT_AMT_SUM,
    ROUND(AVG(c.CONTRACT_AMT), 0)   AS CONTRACT_AMT_AVG,
    COALESCE(SUM(p.PAYMENT_AMT), 0) AS PAY_AMT_SUM,
    COUNT(p.PAYMENT_ID)             AS PAY_CNT
FROM TB_CONTRACT c
LEFT JOIN TB_PAYMENT p
    ON c.CONTRACT_ID = p.CONTRACT_ID
GROUP BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
ORDER BY
    c.CLS_YYMM,
    c.PRODUCT_CD,
    c.STATUS
