/*

Stored Procedure: Load Bronze Layer (Source -> Bronze)
Script Purpose:
  This loads data into the bronze schema from external CSV files, truncates existing tables, and uses BULK INSERT to load data.

Parameters:
None.

Usage Example
  EXEC bronze.load_bronze;

*/

-- Setting up a Stored Procedure
-- You can automate this script later on if needed.
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- Defining variables to track loading times
	BEGIN TRY
	-- Make the table empty THEN load from data source(s)
		PRINT '====================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================';

		PRINT '====================';
		PRINT 'Loading CRM Tables';
		PRINT '====================';
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Eric\Downloads\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- CSV files have row 1 = header
			FIELDTERMINATOR = ',', -- Delimiter(s)
			TABLOCK -- Locks table as it loads it
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Eric\Downloads\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- CSV files have row 1 = header
			FIELDTERMINATOR = ',', -- Delimiter(s)
			TABLOCK -- Locks table as it loads it
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Eric\Downloads\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- CSV files have row 1 = header
			FIELDTERMINATOR = ',', -- Delimiter(s)
			TABLOCK -- Locks table as it loads it
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------------';

		PRINT '====================';
		PRINT 'Loading ERP Tables';
		PRINT '====================';
		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Eric\Downloads\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2, -- CSV files have row 1 = header
			FIELDTERMINATOR = ',', -- Delimiter(s)
			TABLOCK -- Locks table as it loads it
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Eric\Downloads\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2, -- CSV files have row 1 = header
			FIELDTERMINATOR = ',', -- Delimiter(s)
			TABLOCK -- Locks table as it loads it
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Eric\Downloads\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2, -- CSV files have row 1 = header
			FIELDTERMINATOR = ',', -- Delimiter(s)
			TABLOCK -- Locks table as it loads it
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------------';
		
		PRINT '===============================';
		PRINT 'Loading Complete.';
		PRINT 'Total Load Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_start_time) AS NVARCHAR) + ' seconds';
		PRINT '===============================';

	END TRY
	BEGIN CATCH
		PRINT '======================';
		PRINT 'ERROR OCCURRED IN LOAD'
		PRINT 'Message' + ERROR_MESSAGE();
		PRINT 'Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================';
	END CATCH
END

EXEC bronze.load_bronze
-- Setting up a Stored Procedure
-- You can automate this script later on if needed.
