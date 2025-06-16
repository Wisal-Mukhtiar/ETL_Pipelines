CREATE DATABASE IF NOT EXISTS sales_db;

USE sales_db;

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY
);
        
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(30) PRIMARY KEY,
    product_name VARCHAR(150),
    category VARCHAR(100),
    price DECIMAL(10,2)
);
        
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(30),
    quantity INT,
    date VARCHAR(100),
    date_std DATE,
    region VARCHAR(100),
    total_value DECIMAL(12,2),
    has_missing_customer BOOLEAN,
    had_negative_quantity BOOLEAN,
    had_date_format_issue BOOLEAN,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Below is how each index will help in speeding up the queries
-- For regional sales summaries
CREATE INDEX idx_transactions_region ON transactions(region);

-- For monthly sales trends
CREATE INDEX idx_transactions_date_std ON transactions(date_std);

-- For joining with products (especially for aggregations)
CREATE INDEX idx_transactions_product_id ON transactions(product_id);

-- Query 1 after optimization 
WITH filtered_transactions AS (
    SELECT region, total_value
    FROM transactions
    WHERE total_value > 0  -- Skip meaningless rows
)
SELECT region, SUM(total_value) AS total_sales
FROM filtered_transactions
GROUP BY region
ORDER BY total_sales DESC;


-- query 2 (It can be written with a CTE as well)
SELECT 
    p.product_id,
    p.product_name,
    SUM(t.total_value) AS total_sales
FROM transactions t
JOIN products p ON t.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sales DESC
LIMIT 5;

--Query 3
SELECT 
    DATE_FORMAT(date_std, '%Y-%m') AS month,
    SUM(total_value) AS monthly_sales
FROM transactions
WHERE date_std IS NOT NULL
GROUP BY month
ORDER BY month;