-- db/schema.sql

-- Dimension Tables

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price NUMERIC(10, 2)
);

CREATE TABLE dim_branch (
    branch_key SERIAL PRIMARY KEY,
    branch_id INT UNIQUE,
    branch_name VARCHAR(255),
    location VARCHAR(100)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL
);

-- Fact Tables

CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    branch_key INT NOT NULL,
    quantity_sold INT,
    sale_amount NUMERIC(12, 2),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (branch_key) REFERENCES dim_branch(branch_key)
);

CREATE TABLE fact_inventory (
    inventory_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL, -- For snapshot date
    product_key INT NOT NULL,
    branch_key INT NOT NULL,
    stock_on_hand INT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (branch_key) REFERENCES dim_branch(branch_key)
);