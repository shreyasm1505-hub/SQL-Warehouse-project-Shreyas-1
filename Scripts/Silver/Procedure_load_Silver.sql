/*
===================================================================================================================
Stored Procedure: Load Silver Layer (Bronz---> Silver)
===================================================================================================================
Script Purpose:
This stored Procedure performs the ETL Process(Extract,Transform,Load) to populate the Silver schema tables 
from Bronz schema.

Action Performed:
- Truncates Silver tables
-Inserts cleansed and transformed data from bronz to silver schema tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_Silver

===================================================================================================================
*/

/* ===========================================================================

1st TABLE SILVER.CRM_CUST_INFO : 

   =========================================================================== */
CREATE OR ALTER PROCEDURE Silver.load_Silver As

BEGIN 

DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();
        PRINT '===========================================================================';
        PRINT 'LOADING Silver Layer';
        PRINT '===========================================================================';

        PRINT '---------------------------------------------------------------------------';
        PRINT 'Loading CRM TABLES';
        PRINT '---------------------------------------------------------------------------';

       --Loading SILVER.CRM_CUST_INFO
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.crm_cust_info';
        TRUNCATE TABLE Silver.crm_cust_info;
        PRINT '>> INSERTING DATA INTO: Silver.crm_cust_info';
        INSERT INTO Silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END AS cst_material_status, 
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'N/A'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
            FROM Bronz.crm_cust_info
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE() ;
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------------';


        /* ======================================================================================

          2nd TABLE SILVER.CRM_PRD_INFO

          ======================================================================================= */
            -- LOADING Silver.CRM_PRD_INFO
            
            SET @batch_start_time = GETDATE();
            PRINT '>> Truncating Table: Silver.crm_prd_info';
            TRUNCATE TABLE Silver.crm_prd_info;
            PRINT '>> INSERTING DATA INTO: Silver.crm_prd_info';

            INSERT INTO Silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key, 
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt)

        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,--Map product line codes to descriptive values
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
                AS DATE
            ) AS prd_end_dt --- calculate end date as one day before the next start date
           FROM Bronz.crm_prd_info;

           SET @end_time = GETDATE() ;
           PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
           PRINT '---------------------------------';


        /* =================================================================================================

          3rd TABLE SILVER.CRM_SALES_DETAILS

           ================================================================================================= */
           --LOADING SILVER.CRM_SALES_DETAILS

           SET @batch_start_time = GETDATE();
           PRINT '>> Truncating Table: Silver.crm_sales_details';
           TRUNCATE TABLE Silver.crm_sales_details;
           PRINT '>> INSERTING DATA INTO: Silver.crm_sales_details';

           INSERT INTO Silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        )
        SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN (sls_order_dt) != 8  THEN NULL   -- (HERE WE ARE HANDLING "INVALID DATA")
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)			   -- (DATA TYPE CASTING FOR CORRECT DATA )
        END As sls_order_dt,

        CASE WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) != 8  THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END As sls_ship_dt,

        CASE WHEN sls_due_dt = 0 OR LEN (sls_due_dt) != 8  THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END As sls_due_dt,

        CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!= sls_quantity * ABS(sls_price)   --(HERE IF U SEE WE ARE HANDLING THE MISSING DATA & INVALID DATA & INCORRECT DATA)
	         THEN sls_quantity * sls_price
	         ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <=0  
	         THEN sls_sales / NULLIF (sls_quantity,0)
	         ELSE sls_price								--(DERIVE PRICE IF THE ORIGINAL VALUE IS INVALID)
        END AS sls_price
        FROM Bronz.crm_sales_details;

           SET @end_time = GETDATE() ;
           PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
           PRINT '---------------------------------';


        /* ================================================================================================

             4th TABLE SILVER.ERP_CUST_AZ12

           ================================================================================================ */
           --LOADING SILVER.ERP_CUST_AZ12

            PRINT '-----------------------';
            PRINT ' LOADING ERP TABLES';
            PRINT '-----------------------';

            SET @batch_start_time = GETDATE();
            PRINT '>> Truncating Table: Silver.erp_cust_az12';
            TRUNCATE TABLE Silver.erp_cust_az12;
            PRINT '>> INSERTING DATA INTO: Silver.erp_cust_az12';

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
        FROM bronz.erp_cust_az12;

           SET @end_time = GETDATE() ;
           PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
           PRINT '---------------------------------';

        /* ===========================================================================================================
  
           5th TABLE SIVER.ERP_PRD_A101

           ============================================================================================================ */
           --LOADING SILVER.ERP_LOC_A101

           SET @batch_start_time = GETDATE();
           PRINT '>> Truncating Table: Silver.erp_loc_a101';
           TRUNCATE TABLE Silver.erp_loc_a101;
           PRINT '>> INSERTING DATA INTO: Silver.erp_loc_a101';

           INSERT INTO Silver.erp_loc_a101(cid,cntry)
        SELECT 
        REPLACE(cid,'-','') cid,                                    -- FIRST WE HAVE HANDLED INVALID VALUES HERE)
        CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
             WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
             WHEN TRIM(cntry)= '' OR cntry IS NULL THEN 'N/A'
             ELSE TRIM(cntry)                                       -- (NORMALIZE & HANDLE MISSING OR BLANK VALUES)
        END  As cntry
        FROM Bronz.erp_loc_a101;

           SET @end_time = GETDATE() ;
           PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
           PRINT '---------------------------------';

        /* =============================================================================================================
  
           6th TABLE SILVER.ERP_PX_CAT_G1V2

           ============================================================================================================= */
           --LOADING SILVER.ERP_PX_CAT_G1V2

            SET @batch_start_time = GETDATE();
            PRINT '>> Truncating Table: Silver.erp_px_cat_g1v2';
            TRUNCATE TABLE Silver.erp_px_cat_g1v2;
            PRINT '>> INSERTING DATA INTO: Silver.erp_px_cat_g1v2';

           INSERT INTO Silver.erp_px_cat_g1v2(
         id,cat,subcat,maintenance)
        SELECT
        id,
        cat,
        subcat,
        maintenance
        FROM Bronz.erp_px_cat_g1v2;

           SET @end_time = GETDATE() ;
           PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
           PRINT '---------------------------------';
       
        SET @batch_end_time = GETDATE();
        PRINT '================================='
        PRINT 'Loading Silver Layer is complete'
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR ) + ' seconds';
        PRINT '================================='

    END TRY
      BEGIN CATCH
        PRINT '======================================================================';
        PRINT ' ERROR OCCURED WHILE LOADING SILVER LAYER';
        PRINT ' Error Message' + ERROR_MESSAGE();
        PRINT ' Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT ' Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '======================================================================';
      END CATCH
  
  END 
