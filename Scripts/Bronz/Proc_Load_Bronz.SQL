/*
==============================================================================================================
Stored Procedure: Load Bronz Layer (SOurce-> Bronz)
==============================================================================================================
Script Purpose:
This stored procedure loads the data into the 'Bronz'Schema from external ".CSV" files.
It performs the following actions
  - Truncates the bronz layer before loading the data
  - Uses the BULK INSERT command to insert data from the CSV file to bronz tables.

Parameters:
None.
This stored Procedure doest not accept any parameters or return any values.

Usage examples:
EXEC Bronz.load_Bronz;
==============================================================================================================
*/

CREATE OR ALTER PROCEDURE Bronz.load_Bronz AS

BEGIN
	DECLARE @Start_time DATETIME, @End_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
			PRINT '==================================================';
			PRINT 'Loading into the BRONZ LAYER';
			PRINT '==================================================';

			PRINT '--------------------------------------------------';
			PRINT ' Loading  CRM Tables';
			PRINT '--------------------------------------------------';

			SET @Start_time = GETDATE();
			PRINT '>>Truncating the table: Bronz.crm_cust_info';
			TRUNCATE TABLE Bronz.crm_cust_info;

			PRINT '>>INSERTING the DATA into the table: Bronz.crm_cust_info';
			BULK INSERT Bronz.crm_cust_info
			from 'C:\Users\SHREYAS\Documents\Shreyas SQL\sql_warehouse_project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
			   FIRSTROW = 2,
			   FIELDTERMINATOR = ',',
			   TABLOCK 
			);
			SET @End_time = GETDATE ();
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF (second,@Start_time,@End_time) AS NVARCHAR) + ' Seconds';
			PRINT '---------------------------------------------------------------------------------------------';

			SET @Start_time = GETDATE();
			PRINT '>>TRUNCATING the DATA into the table:Bronz.crm_prd_info';
			TRUNCATE TABLE Bronz.crm_prd_info;

			PRINT '>>INSERTING the DATA into the table: Bronz.crm_prd_info';
			BULK INSERT Bronz.crm_prd_info
			from 'C:\Users\SHREYAS\Documents\Shreyas SQL\sql_warehouse_project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @End_time = GETDATE ();
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF (second,@Start_time,@End_time) AS NVARCHAR) + ' Second';
			PRINT '---------------------------------------------------------------------------------------------';


			SET @Start_time = GETDATE();
			PRINT '>>TRUNCATING the DATA into the table: Bronz.crm_sales_details';
			TRUNCATE TABLE Bronz.crm_sales_details;

			PRINT '>>INSERTING the DATA into the table: Bronz.crm_sales_details';
			BULK INSERT Bronz.crm_sales_details
			from 'C:\Users\SHREYAS\Documents\Shreyas SQL\sql_warehouse_project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @End_time = GETDATE ();
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF (second,@Start_time,@End_time) AS NVARCHAR) + ' Second';
			PRINT '---------------------------------------------------------------------------------------------';

			PRINT '--------------------------------------------------';
			PRINT ' Loading  ERP Tables';
			PRINT '--------------------------------------------------';

			SET @Start_time = GETDATE();
			PRINT '>> Truncating the Table: Bronz.erp_cust_az12';
			TRUNCATE TABLE Bronz.erp_cust_az12;

			PRINT '>> INSERTING the DATA into the Table: Bronz.erp_cust_az12';
			BULK INSERT Bronz.erp_cust_az12
			from 'C:\Users\SHREYAS\Documents\Shreyas SQL\sql_warehouse_project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @End_time = GETDATE ();
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF (second,@Start_time,@End_time) AS NVARCHAR) + ' Second';
			PRINT '---------------------------------------------------------------------------------------------';

			SET @Start_time = GETDATE();
			PRINT '>> Truncating the DATA into the Table: Bronz.erp_loc_a101';
			TRUNCATE TABLE Bronz.erp_loc_a101;

			PRINT '>> INSERTING the DATA into the Table: Bronz.erp_loc_a101';
			BULK INSERT Bronz.erp_loc_a101
			from 'C:\Users\SHREYAS\Documents\Shreyas SQL\sql_warehouse_project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @End_time = GETDATE ();
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF (second,@Start_time,@End_time) AS NVARCHAR) + 'Second';
			PRINT '---------------------------------------------------------------------------------------------';

			SET @Start_time = GETDATE();
			PRINT '>> Truncating the DATA into the Table: Bronz.erp_px_cat_g1v2';
			TRUNCATE TABLE Bronz.erp_px_cat_g1v2;

			PRINT '>> INSERTING the DATA into the Table: Bronz.erp_px_cat_g1v2';
			BULK INSERT Bronz.erp_px_cat_g1v2
			from 'C:\Users\SHREYAS\Documents\Shreyas SQL\sql_warehouse_project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @End_time = GETDATE ();
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF (second,@Start_time,@End_time) AS NVARCHAR) + 'Second';
			PRINT '---------------------------------------------------------------------------------------------';


			SET @batch_end_time = GETDATE();
			PRINT'=============================================================================='
			PRINT'Loading Bronz Layer is completed';
			PRINT '  - TOTAL Duration: ' +CAST(DATEDIFF (SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds' 
			PRINT '=============================================================================='

	 END TRY
	 BEGIN CATCH
			PRINT '============================================================================'
			PRINT 'ERROR OCCURED DURING LOADING THE BRONZ LAYER '
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message'+ CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '============================================================================'

	 END CATCH
END

