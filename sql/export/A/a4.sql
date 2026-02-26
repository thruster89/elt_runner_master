SELECT
    contract_no,
    contract_date,
    product_code,
    rate_code
FROM
    contract_info
    WHERE RATE_CODE = :rateCode
    AND PRODUCT_CODE = :productCode
    ;