/* 
============================================================================================
DDL Script: Create Gold View
============================================================================================
Script Purpose:
This script creates the view for Gold layer in the Data Warehouse.
The Gold Layer represents the final dimensions and fact table (Star schema)


Each view performs transformations and combines data from the Silver layer
to produce a clean,enriched,and business-ready dataset.

Usage:
 - These views can be queried directly for analytics and reporting.

 ===========================================================================================
 */

/*======================================================================================
1st view: Create VIEW Gold.dim_customers
========================================================================================
*/

IF OBJECT_ID('Gold.dim_customers','V') IS NOT NULL
    DROP VIEW Gold.dim_customers
GO
Create VIEW Gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname As first_name,
ci.cst_lastname AS last_name,
la.cntry As country,
ci.cst_material_status AS martial_status,
CASE WHEN ci.cst_gndr !='N/A' THEN ci.cst_gndr -- CRM is the Master for Gender info
     ELSE COALESCE (ca.gen,'N/A')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN SIlver.erp_cust_az12 ca
on  ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

GO

/*
================================================================================================
2nd View: CREATE VIEW Gold.dim_products
================================================================================================
*/
IF OBJECT_ID('Gold.dim_products','V') IS NOT NULL
    DROP VIEW Gold.dim_products
GO

CREATE VIEW Gold.dim_products AS
SELECT 
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_id ) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm As product_name,
	pn.cat_id As category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost As cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt As start_date
FROM Silver.crm_prd_info pn
LEFT JOIN Silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL  -- FIltered out all the Historical Data;

GO

/* 
==================================================================================================
3rd View: CREATE VIEW Gold.facy_sales
==================================================================================================
*/
IF OBJECT_ID('Gold.facy_sales','V') IS NOT NULL
    DROP VIEW Gold.facy_sales
GO

CREATE VIEW Gold.facy_sales AS
SELECT
sd.sls_ord_num As order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt As due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM Silver.crm_sales_details sd
LEFT JOIN Gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN Gold.dim_customers cu
ON sd.sls_cust_id  = cu.customer_id;

GO

