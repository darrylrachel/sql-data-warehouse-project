-- Check for NULLS or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id,
       COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT *
FROM (
    SELECT *,
       ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
     )t WHERE flag_last = 1;


-- Standardization
    -- Removing white space
    -- Expectation: No Result

SELECT cst_firstname,
       cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


-- Data Consistnecy
SELECT DISTINCT cst_gender,
       CASE WHEN UPPER(cst_gender) = 'F' THEN 'Female'
            WHEN UPPER(cst_gender) = 'M' THEN 'Male'
            ELSE 'n/a'
        END cst_gender
FROM bronze.crm_cust_info;


-- Data Consistnecy
SELECT DISTINCT cst_marital_status,
       CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
            WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
            ELSE 'n/a'
        END cst_marital_status
FROM bronze.crm_cust_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt)

SELECT prd_id,
       SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
       REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
       prd_nm,
       ISNULL(prd_cost, 0) AS prd_cost,
       CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
       END AS prd_line, -- Map product line codes to descriptive values
       CAST (prd_start_dt AS DATE) AS prd_start_dt,
       CAST (
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
            AS DATE) AS prd_end_dt -- Calculate end date as one day before the next start date
FROM bronze.crm_prd_info;

SELECT prd_id,
       COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


SELECT prd_key,
       REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
       SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
(SELECT DISTINCT cid FROM bronze.erp_px_cat_g1v2);



SELECT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_sales)
            THEN sls_quantity * ABS(sls_sales)
         ELSE sls_sales
    END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
         ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_quantity, sls_price;


-- Silver Layer
SELECT cst_id,
       COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;