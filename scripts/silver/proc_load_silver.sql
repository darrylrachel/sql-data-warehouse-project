/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
EXEC silver.load_silver;
CREATE OR ALTER PROCEDURE  silver.load_silver AS
    BEGIN
        DECLARE @start_time DATETIME,
                @end_time DATETIME,
                @batch_start_time DATETIME,
                @batch_end_time DATETIME;

        BEGIN TRY
            SET @batch_start_time = GETDATE();
            PRINT '================================================';
            PRINT 'Loading Silver Layer';
            PRINT '================================================';
            PRINT '';

            PRINT '-------------------------------------------------';
            PRINT 'Loading CRM TABLES';
            PRINT '-------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.crm_cust_info'
            TRUNCATE TABLE silver.crm_cust_info;

            PRINT '>> Inserting Data Into: silver.crm_cust_info'
            INSERT INTO silver.crm_cust_info (
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gender,
                cst_create_date)

            SELECT cst_id,
                   cst_key,
                   TRIM(cst_firstname) AS cst_firstname,
                   TRIM(cst_lastname) AS cst_lastname,
                   CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                        ELSE 'n/a'
                   END cst_marital_status,
                   CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
                        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
                        ELSE 'n/a'
                    END cst_gender,
                   cst_create_date
            FROM (
                SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL
                 )t WHERE flag_last = 1;
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.crm_sales_details'
            TRUNCATE TABLE silver.crm_sales_details;
            PRINT '>> Inserting Data Into: silver.sales_details'
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
                CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                     ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
                END AS sls_order_dt,
                CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                     ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
                END AS sls_ship_dt,
                CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                     ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
                END AS sls_due_dt,
                CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_sales)
                         THEN sls_quantity * ABS(sls_sales)
                     ELSE sls_sales
                END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
                sls_quantity,
                CASE WHEN sls_price IS NULL OR sls_price <= 0
                        THEN sls_sales / NULLIF(sls_quantity, 0)
                     ELSE sls_price
                END AS sls_price
            FROM bronze.crm_sales_details;
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-------------------------------------------------';

            PRINT '-------------------------------------------------';
            PRINT 'Loading ERP TABLES';
            PRINT '-------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.erp_cust_az12'
            TRUNCATE TABLE silver.erp_cust_az12;
            PRINT '>> Inserting Data Into: silver.erp_cust_az12'
            INSERT INTO silver.erp_cust_az12(
                cid,
                bdate,
                gen
            )
            SELECT
                CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                     ELSE cid
                END AS cid,
                CASE WHEN bdate > GETDATE() THEN NULL
                     ELSE bdate
                END AS bdate,
                CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
                     WHEN UPPER(TRIM(gen))  IN ('M', 'Male') THEN 'Male'
                     ELSE 'n/a'
                END as gen
            FROM bronze.erp_cust_az12;
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.erp_loc_a101'
            TRUNCATE TABLE silver.erp_loc_a101;
            PRINT '>> Inserting Data Into: silver.erp_loc_a101'
            INSERT INTO silver.erp_loc_a101 (cid, cntry)
            SELECT
                REPLACE(cid, '-', '') AS cid,
                CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                     WHEN TRIM(cntry) = '' OR  cntry IS NULL THEN 'n/a'
                     ELSE TRIM(cntry)
                END AS cntry
            FROM bronze.erp_loc_a101;
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
            INSERT INTO silver.erp_px_cat_g1v2 (
                cid,
                cat,
                subcat,
                maintenance
            )
            SELECT
                cid,
                cat,
                subcat,
                maintenance
            FROM bronze.erp_px_cat_g1v2;

            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-------------------------------------------------';

            SET @batch_end_time = GETDATE();
            PRINT 'Total Batch Completed';
            PRINT '>> Batch Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
            PRINT '-------------------------------------------------';
        END TRY
        BEGIN CATCH
            PRINT '===========================================';
            PRINT 'ERROR OCCURED DURING SILVER LAYER LOADING'
            PRINT 'Error Message' + CAST (ERROR_MESSAGE() AS NVARCHAR);
            PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
            PRINT '===========================================';
        END CATCH
    END;

