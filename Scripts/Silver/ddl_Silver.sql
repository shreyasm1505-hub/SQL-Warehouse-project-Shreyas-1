/* 
=====================================================================================================================================
DDL Script: Create Silver Layer
=====================================================================================================================================
Script Purpose:
This script creates tables in the 'Silver' schema, droping existing tables
if they already exist.
Run this script to re-define the DDL structure of 'Bronz' Tables.
=====================================================================================================================================
*/

if OBJECT_ID ('Silver.crm_cust_info','U') IS NOT NULL
DROP TABLE Silver.crm_cust_info;

Go
  
Create table Silver.crm_cust_info(
cst_id int,
cst_key NVarchar(50),
cst_firstname NVarchar(50),
cst_lastname NVarchar (50),
cst_material_status Nvarchar(50),
cst_gndr Nvarchar(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO
  
if OBJECT_ID ('Silver.crm_prd_info','U') IS NOT NULL
DROP TABLE Silver.crm_prd_info;

Go

 Create table Silver.crm_prd_info(
 prd_id int,
 prd_key Nvarchar(50),
 prd_nm Nvarchar(50),
 prd_cost int,
 prd_line Nvarchar (50),
 prd_start_dt DATEtime,
 prd_end_dt DATEtime ,
 dwh_create_date DATETIME2 DEFAULT GETDATE()
 );

GO
  
 if OBJECT_ID ('Silver.crm_sales_details','U') IS NOT NULL
DROP TABLE Silver.crm_sales_details;

GO
  
 create table Silver.crm_sales_details(
 sls_ord_num Nvarchar (50),
 sls_prd_key Nvarchar (50),
 sls_cust_id	int,
 sls_order_dt	int,
 sls_ship_dt	int,
 sls_due_dt	 int,
 sls_sales	int,
 sls_quantity	int,
 sls_price int,
 dwh_create_date DATETIME2 DEFAULT GETDATE()
 );

GO
  
 if OBJECT_ID ('Silver.erp_cust_az12','U') IS NOT NULL
DROP TABLE Silver.erp_cust_az12;

GO
  
 create table Silver.erp_cust_az12(
 cid Nvarchar (50),
 bdate Date,
 gen Nvarchar(50),
 dwh_create_date DATETIME2 DEFAULT GETDATE()
 );

GO
  
 if OBJECT_ID ('Silver.erp_loc_a101','U') IS NOT NULL
DROP TABLE Silver.erp_loc_a101;

GO
  
 create table  Silver.erp_loc_a101(
 cid Nvarchar (50),
 cntry Nvarchar (50),
 dwh_create_date DATETIME2 DEFAULT GETDATE()
 );

GO
  
 if OBJECT_ID ('Silver.erp_px_cat_g1v2','U') IS NOT NULL
DROP TABLE Silver.erp_px_cat_g1v2;

GO
  
 create table Silver.erp_px_cat_g1v2(
 id Nvarchar (50),
 cat Nvarchar (50),
 subcat Nvarchar (50),
 maintenance Nvarchar (50),
 dwh_create_date DATETIME2 DEFAULT GETDATE()
 );

GO

