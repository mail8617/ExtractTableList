/* [PostgreSQL] 
   1. CTE(WITH절) 임시 테이블 제외 테스트
   2. 스키마명 포함 테이블 추출 테스트
*/
WITH monthly_sales AS (
    SELECT product_id, SUM(amount) AS total_sales
    FROM sales.transactions
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 month'
    GROUP BY 1
)
SELECT
    p.product_name,
    ms.total_sales,
    c.category_name
FROM a.products p
-- 판매 실적 임시 테이블과 조인 (이 주석 안의 테이블명은 무시되어야 함)
JOIN monthly_sales ms ON p.id = ms.product_id
LEFT JOIN (
    SELECT id, category_name 
    FROM a.categories
) c ON p.category_id = c.id
WHERE p.status = 'ACTIVE';