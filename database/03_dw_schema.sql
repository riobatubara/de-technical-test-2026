-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    week_of_year INT,
    is_weekend BOOLEAN NOT NULL,
    is_month_start BOOLEAN,
    is_month_end BOOLEAN
);

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    city VARCHAR(100) NOT NULL,
    signup_date DATE NOT NULL
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT NOT NULL UNIQUE,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10,2) NOT NULL
);

-- Dimension: Campaign
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id INT NOT NULL UNIQUE,
    campaign_name VARCHAR(150) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    channel VARCHAR(50) NOT NULL
);

-- Default row for "No Campaign"
INSERT INTO dim_campaign (campaign_id, campaign_name, start_date, end_date, channel)
VALUES (0, 'No Campaign', CURRENT_DATE, CURRENT_DATE, 'N/A');

-- Fact: sales
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGSERIAL,

    transaction_date_key INT NOT NULL REFERENCES dim_date(date_key),
    customer_key INT NOT NULL REFERENCES dim_customer(customer_key),
    product_key INT NOT NULL REFERENCES dim_product(product_key),
    campaign_key INT NOT NULL DEFAULT 1 REFERENCES dim_campaign(campaign_key),

    transaction_id INT NOT NULL,
    transaction_item_id INT NOT NULL,

    quantity INT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    gross_amount NUMERIC(12,2) NOT NULL,
    allocated_total_amount NUMERIC(12,2) NOT NULL,

    PRIMARY KEY (sales_key, transaction_date_key),

    CONSTRAINT uq_transaction_line 
        UNIQUE (transaction_id, transaction_item_id, transaction_date_key),

    CONSTRAINT chk_gross_amount 
        CHECK (gross_amount = quantity * unit_price)
)
PARTITION BY RANGE (transaction_date_key);

-- Indexes
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product  ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_campaign ON fact_sales(campaign_key);