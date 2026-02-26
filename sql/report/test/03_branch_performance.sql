-- 지점별 분기 실적
SELECT
    bp.REGION                   AS "지역",
    bp.BRANCH_NAME              AS "지점명",
    bp.CLS_YYMM                 AS "분기",
    bp.AGENT_CNT                AS "설계사수",
    bp.CONTRACT_CNT             AS "계약건수",
    bp.CONTRACT_AMT_SUM         AS "계약금액",
    ROUND(bp.CONTRACT_AMT_SUM * 1.0 / NULLIF(bp.AGENT_CNT, 0), 0) AS "인당생산성"
FROM RPT_BRANCH_PERF bp
ORDER BY bp.CLS_YYMM, bp.CONTRACT_AMT_SUM DESC
