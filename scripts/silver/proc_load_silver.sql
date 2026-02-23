/*

Stored Procedure: Load Silver Layer (Bronze -> Silver
Script Purpose:
  Performs ETL process to populate silver schema from bronze schema.
Actions:
  Truncates Silver Tables
  Inserts transformed/cleansed data from Bronze into Silver.

Parameters:
None.

Usage Example
  EXEC silver.load_silver;

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	/*
	Notice: This is for "crm_cust_info" table.
	*/
	TRUNCATE TABLE silver.crm_cust_info;

	WITH ranked AS (
		SELECT 
			*,
			ROW_NUMBER() OVER (
				PARTITION BY cst_id
				ORDER BY cst_create_date DESC
			) AS flag_last
		FROM bronze.crm_cust_info
	)

	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)

	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END cst_marital_status,
		-- Removes excess spaces and changes abbreviations into full details
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A'
		END cst_gndr,
		cst_create_date
	FROM ranked
	WHERE flag_last = 1 AND cst_id IS NOT NULL

	/*
	Notice: This is for "crm_prd_info" table.
	*/
	TRUNCATE TABLE silver.crm_prd_info;

	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- The first 5 characters are the "category ID"
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- LEN works for variable inputs and joins with another table.
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, -- assuming safe to assign a value rather than delete the entire row
	CASE UPPER(TRIM(prd_line)) -- viable with 1-to-1 mapping on CASE statements
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt, -- Removing "times" to dates
	-- Fixing start date < end date
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS prd_end_dt_test
	FROM bronze.crm_prd_info

	-- the table to "check" with "cat_id" column
	-- WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

	-- the table to "check" with "cat_id" column
	-- WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (SELECT sls_prd_key FROM bronze.crm_sales_details)

	/*
	Notice: This is for "crm_sales_details" table.
	*/
	TRUNCATE TABLE silver.crm_sales_details;

	INSERT INTO silver.crm_sales_details (
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
		-- Order Date < Shipping OR Due Date
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- Double cast due to SQL Server
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- Double cast due to SQL Server
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- Double cast due to SQL Server
		END AS sls_due_dt,
		-- Sales = Quantity * Price. No Negative, Zero, or Nulls allowed.
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity,0) -- Stops division by 0.
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details

	/*
	Notice: This is for "erp_cust_az12" table.
	*/
	TRUNCATE TABLE silver.erp_cust_az12;

	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)

	SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- ID starts with 3 chars
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'N/A'
		END AS gen
	FROM bronze.erp_cust_az12

	/*
	Notice: This is for "erp_loc_a101" table.
	*/

	TRUNCATE TABLE silver.erp_loc_a101;

	INSERT INTO silver.erp_loc_a101
	(cid, cntry)

	SELECT
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101

	/*
	Notice: This is for "erp_px_cat_g1v2" table.
	*/

	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	-- Table is already "clean" by Silver standards.
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2
END

EXEC silver.load_silver
