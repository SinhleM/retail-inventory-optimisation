-- db/sample_queries.sql

-- 1. Total sales amount per branch
SELECT
    db.branch_name,
    SUM(fs.sale_amount) as total_sales
FROM fact_sales fs
JOIN dim_branch db ON fs.branch_key = db.branch_key
GROUP BY db.branch_name
ORDER BY total_sales DESC;

-- 2. Top 10 best-selling products by quantity
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.quantity_sold) as total_quantity_sold
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name, dp.category
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- 3. Monthly sales trend for the 'Electronics' category
SELECT
    dd.year,
    dd.month,
    SUM(fs.sale_amount) as monthly_electronics_sales
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
JOIN dim_product dp ON fs.product_key = dp.product_key
WHERE dp.category = 'Electronics'
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;

-- 4. Current stock levels for a specific product (e.g., product_id=1) across all branches
SELECT
    db.branch_name,
    dp.product_name,
    fi.stock_on_hand
FROM fact_inventory fi
JOIN dim_branch db ON fi.branch_key = db.branch_key
JOIN dim_product dp ON fi.product_key = dp.product_key
WHERE dp.product_id = 1
ORDER BY fi.stock_on_hand DESC;