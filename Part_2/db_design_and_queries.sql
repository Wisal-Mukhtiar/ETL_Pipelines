-- This sql file will bring the data
-- from the same table that was inserted in part 1
-- Python's script :)

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


-- Data Insertion from the already created sales table from step 1
-- here we are inserting the data from the already created table in part 1
INSERT INTO products (product_id, product_name, category, price)
SELECT DISTINCT product_id, product_name, category, price
FROM sales
WHERE product_id IS NOT NULL;


INSERT INTO customers (customer_id)
SELECT DISTINCT customer_id
FROM sales
WHERE customer_id IS NOT NULL;


INSERT INTO customers (customer_id)
SELECT DISTINCT 'Unknown'
FROM sales
WHERE customer_id IS NULL;

INSERT INTO transactions (
    transaction_id, customer_id, product_id,
    quantity, date, date_std, region,
    total_value, has_missing_customer,
    had_negative_quantity, had_date_format_issue
)
SELECT
    transaction_id,
    COALESCE(customer_id, 'Unknown'),  -- handle nulls
    product_id,
    quantity,
    date,
    date_std,
    region,
    total_value,
    has_missing_customer,
    had_negative_quantity,
    had_date_format_issue
FROM sales;

-- Add an index on 'region' to speed up region-based queries
CREATE INDEX idx_region ON transactions(region);


-- Query 1
SELECT region, SUM(total_value) AS total_sales
FROM transactions
GROUP BY region
ORDER BY total_sales DESC;

-- Query 2
SELECT 
    p.product_id,
    p.product_name,
    SUM(t.total_value) AS total_sales
FROM transactions t
JOIN products p ON t.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sales DESC
LIMIT 5;

-- Query 3
SELECT 
    DATE_FORMAT(date_std, '%Y-%m') AS month,
    SUM(total_value) AS monthly_sales
FROM transactions
WHERE date_std IS NOT NULL
GROUP BY month
ORDER BY month;
