-- 분기별 사업 현황 대시보드
SELECT
    s.CLS_YYMM                          AS "분기",
    s.CATEGORY                          AS "상품군",
    SUM(s.CNT)                          AS "계약건수",
    SUM(s.AMT_SUM)                      AS "계약금액합계",
    ROUND(AVG(s.AMT_AVG), 0)           AS "평균계약금액",
    COALESCE(ca.CLAIM_CNT, 0)          AS "청구건수",
    COALESCE(ca.CLAIM_AMT_SUM, 0)      AS "청구금액합계",
    lr.LOSS_RATIO_PCT                   AS "손해율(%)"
FROM RPT_CONTRACT_SUMMARY s
LEFT JOIN (
    SELECT CLS_YYMM, SUM(CLAIM_CNT) AS CLAIM_CNT, SUM(CLAIM_AMT_SUM) AS CLAIM_AMT_SUM
    FROM RPT_CLAIM_ANALYSIS
    GROUP BY CLS_YYMM
) ca ON s.CLS_YYMM = ca.CLS_YYMM
LEFT JOIN RPT_LOSS_RATIO lr ON s.CLS_YYMM = lr.CLS_YYMM
GROUP BY s.CLS_YYMM, s.CATEGORY, ca.CLAIM_CNT, ca.CLAIM_AMT_SUM, lr.LOSS_RATIO_PCT
ORDER BY s.CLS_YYMM, s.CATEGORY
