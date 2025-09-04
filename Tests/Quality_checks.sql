/*
========================================================================================================================
Quality checks
========================================================================================================================
Script Purpose:
      This script performs variuos quality checks for data consistency,accuracy, 
      and standardization across the 'Silver' schema.It includes checks for:
            - Null or duplicate Primary_key
            - Unwanted spaces in string fields
            - Data Standardization and consistency.
            - Invalid date range and orders.
            - Data consistency between related fields

Usage notes: 
 - Run these checks after data is loaded into 'Silver_Layer'.
 - investigate and resolve any descripancies found during the check.
=========================================================================================================================
*/

-- Check for NULLs Or Duplicates in Primary key
-- Expectation: No Result


Select
cst_id,
COunt(*)
from bronz.crm_cust_info
Group by cst_id
Having COUNT(*)>1 OR cst_id IS NULL;


-- Check for unwanted Spaces
-- Expectation: No Results

SELECT cst_key
FROM Bronz.crm_cust_info
WHERE cst_key != TRIM(cst_key)


--Data Standardization & Consistency
SELECT DISTINCT cst_material_status
FROM Bronz.crm_cust_info

  

  --FOR testing SILVER LAYER 
  --Check For Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT
cst_id,
COUNT(*)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


--Check for NULLS or negative values
-- Expectation: No Result
SELECT prd_cost
FROM Bronz.crm_prd_info
where prd_cost<0 OR prd_cost IS NULL


-- Check for unwanted Spaces
-- Expectation: No Results

SELECT cst_lastname
FROM Silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM Silver.crm_cust_info

Select*From Silver.crm_cust_info;

DELETE FROM Silver.crm_cust_info
WHERE cst_id IS NULL;




-- CHECK for unwanted SPACES
-- and also fro gender instead of abbrivation we use full meaningful words.
Select
cst_lastname
from bronz.crm_cust_info
where cst_firstname!=Trim(cst_lastname)



INSERT INTO Silver.crm_cust_info

--==========================================================
--TEST CASE FOR Checking the country in (Silver.erp_loc_a101)

 (SELECT
cntry,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
     WHEN TRIM(cntry)= '' OR cntry IS NULL THEN 'N/A'
     ELSE TRIM(cntry)
END  As cntry
from bronz.erp_loc_a101) 


-- ===================================================
-- ===================================================

SELECT DISTINCT
cntry As OLD_cntry,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
     WHEN TRIM(cntry)= '' OR cntry IS NULL THEN 'N/A'
     ELSE TRIM(cntry)
END  As cntry
from bronz.erp_loc_a101
ORDER BY cntry;

--Business Rule:
--SALES = QUANTITY * PRICE
-->> Values should not be Negative,Zero & NULL 

SELECT DISTINCT
sls_sales as old_sls_slaes,
sls_quantity,
sls_price as ols_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!= sls_quantity * ABS(sls_price) 
	 THEN sls_quantity * sls_price
	 ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0  
	 THEN sls_sales / NULLIF (sls_quantity,0)
	 ELSE sls_price
END AS sls_price

FROM Bronz.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
ORDER BY sls_sales,sls_quantity,sls_price;


-- CHWECKING WETHER OUR SILVER LAYER IS FOLLOWING ALL THE BUSINESS RUKES PROPERLY OR NOT

SELECT DISTINCT
sls_sales as old_sls_slaes,
sls_quantity,
sls_price as ols_sls_price

FROM Silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
ORDER BY sls_sales,sls_quantity,sls_price;

SELECT* FROM Silver.crm_sales_details;


-- CLEAN & LOAD ( ERP_CUST_AZ12 )
INSERT INTO Silver.erp_cust_az12(cid,bdate,gen)
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))  ----- (REMOVES "NAS" PREFIX IF PRESENT)
     ELSE cid
END  AS cid,

CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
     END as bdate,                                       -----( SET FUTURE BIRTHDATES TO NULL)
 
CASE WHEN gen collate SQL_Latin1_General_CP1_CS_AS in ('M','m') THEN 'Male'
     WHEN gen collate SQL_Latin1_General_CP1_CS_AS in ('F','f') THEN 'Female'
     WHEN gen IS NULL OR gen = ' ' OR gen ='NULL' THEN 'N/A'
     ELSE gen                                            -----(NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES)
END AS gen
FROM bronz.erp_cust_az12


