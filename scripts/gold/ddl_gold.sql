/*
========================
DDL Script: Create Gold Views
========================
Purpose:
  Generate views for Gold layer in data warehouse
  Tranform and combine data from silver -> gold for business-ready dataset.

Usage:
  Query directly for analytics.
*/

/* 
Designing the Customer Table in the Gold Layer
*/

-- Selecting columns to put in gold layer, sorting them, and renaming columns to be "friendly" with snake_case.
-- Avoiding INNER JOINs because unsure if I'll lose customers that way.
-- Silver layer covers many steps, so these scripts may seem simpler out of context.
-- If you see many NULLs or duplicates, a join may be incorrect.

CREATE VIEW gold.dim_customers AS 

SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Surrogate Key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr 
		ELSE COALESCE(ca.gen, 'N/A')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid

-- Checking for Duplicates
/* 
SELECT cst_id, COUNT(*) FROM
([code])t GROUP BY cst_id
HAVING COUNT(*) > 1
*/

/* Checking in general
SELECT * FROM gold.dim_customers */

/* 
Designing the Product Table in the Gold Layer
*/

-- Any NULLS are considered as "current info"

CREATE VIEW gold.dim_products AS

SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate Key
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter historical data

/* Ensure product key is unique
SELECT prd_key, COUNT(*) FROM
([code])t GROUP BY cst_id
HAVING COUNT(*) > 1
*/

/* 
Designing the Sales Table in the Gold Layer
*/

-- Only one table, but need to integrate multiple keys into one.

CREATE VIEW gold.fact_sales AS

SELECT
sd.sls_ord_num AS order_number,
pr.product_key, -- Surrogate key already generated in warehouse
cu.customer_key, -- Surrogate key already generated in warehouse
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id
