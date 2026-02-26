-- 상품별 실적 랭킹 (전체 기간)
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(AMT_SUM) DESC) AS "순위",
    PRODUCT_CD                                      AS "상품코드",
    PRODUCT_NAME                                    AS "상품명",
    CATEGORY                                        AS "상품군",
    SUM(CNT)                                        AS "총계약건수",
    SUM(AMT_SUM)                                    AS "총계약금액",
    ROUND(AVG(AMT_AVG), 0)                         AS "평균계약금액",
    COUNT(DISTINCT CLS_YYMM)                        AS "판매분기수"
FROM RPT_CONTRACT_SUMMARY
WHERE STATUS = '01'
GROUP BY PRODUCT_CD, PRODUCT_NAME, CATEGORY
ORDER BY "총계약금액" DESC
