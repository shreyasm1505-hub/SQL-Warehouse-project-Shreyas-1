/* 
========================================================================================================
 Script Purpose:
 Perform quality checks to validate the integrity, consistency, and accuracy of the Gold Layer.
 These checks ensure:
 1. Uniqueness of surrogate keys in dimension tables.
 2. Referential integrity between fact and dimension tables.
========================================================================================================
*/

--  Checking gold.dim_customers
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results (i.e., no duplicates)

SELECT
 customer_key,
 COUNT(*) AS duplicate_count
 FROM gold.dim_customers
 GROUP BY customer_key
 HAVING COUNT(*) > 1;


-- 2 Checking gold.dim_products
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results (i.e., no duplicates)

SELECT
product_key,
COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


-- 3 Checking gold.fact_sales
-- Check for Referential Integrity between fact and dimensions
-- Expectation: No results (i.e., all fact records link properly)

SELECT
f.order_number,
f.customer_key,
f.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p  ON p.product_key  = f.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;
