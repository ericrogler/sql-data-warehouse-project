/* Bronze Layer -> Silver Layer

These scripts check for "dirty" data, such as nulls or duplicate checking in primary key, 
bad records, and information errors inside entries.

Notice: Adjust the tables/columns as needed. Check before and after silver layer ingestion.
*/

-- Checks for duplicate/null in Primary Key
-- There should be no results for Silver layer.
SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT *
FROM silver.crm_prd_info
WHERE cst_id = 29466
-- Shows that the "newest" creation date is the most accurate/reasonable information

-- Check for unwanted spaces in column(s)
-- There should be no results.
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) -- if value is not equal to same value after trimming, there's spaces.

-- Data Standardization & Consistency
-- i.e. "check for all possible values"
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

SELECT * FROM silver.crm_prd_info

-- Check for NULLs or Negative Numbers
-- There should be no results.
SELECT prd_cost 
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Check for Invalid Date Orders (end date smaller than start date)
-- There should be no results. If so, port small sample to spreadsheet for further analysis.
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_ship_dt

-- Check for Invalid Dates (like integer dates)
-- There should be no results
SELECT 
	NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE 
sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt <= 0

-- Check Data Consistency: Sales, Quantity, and Price
-- >> Sales = Quantity * Pprice
-- >> Values must not be NULL, zero, or negative
-- If there's values here, verify with an expert how to proceed.
	-- If Sales Negative, Zero, Null, derive Quantity * Price
	-- If Price Zero/Null, derive Sales * Quantity
	-- If Price Negative, make positive

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity,0) -- Stops division by 0.
	ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Check for Invalid Dates (like integer dates)
-- For dates where the date cannot be past the present time.
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
-- i.e. "check for all possible values"
-- For CASE statements
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
END AS gen
FROM bronze.erp_cust_az12

-- Data Standardization & Consistency
-- i.e. "check for all possible values"
-- For Countries

SELECT DISTINCT
	cntry AS old_cntry,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry -- Normalize blank country codes
FROM bronze.erp_loc_a101
ORDER BY cntry

-- Check for unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

