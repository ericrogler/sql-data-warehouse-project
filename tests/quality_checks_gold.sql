/*
========================
Quality Checks
========================
Purpose:
  To help perform checks on the quality of the data and determine integrity, consistency, and accuracy of Gold Layer.

Usage:
  Run after loading data from Silver layer.
  After running, investigate and resolve discrepancies.
*/

-- Checking for Data Integration on Customer Dimension Table (Multiple Sources -> One Source)
-- A NULL appears if there's no match.
-- Assume the CRM is the primary source for Gender.

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr 
		ELSE COALESCE(ca.gen, 'N/A')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
ORDER BY 1,2

-- Foreign Key Integrity (By joining dimension tables onto fact table)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
	ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
