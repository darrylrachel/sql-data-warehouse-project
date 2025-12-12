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
        END cst_gender
FROM bronze.crm_cust_info;