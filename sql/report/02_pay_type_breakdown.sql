SELECT
    p.CLS_YYMM,
    p.PAY_TYPE,
    COUNT(p.PAYMENT_ID)             AS PAY_CNT,
    SUM(p.PAY_AMT)                  AS PAY_AMT_SUM,
    ROUND(AVG(p.PAY_AMT), 0)        AS PAY_AMT_AVG
FROM TB_PAYMENT p
GROUP BY
    p.CLS_YYMM,
    p.PAY_TYPE
ORDER BY
    p.CLS_YYMM,
    p.PAY_TYPE
