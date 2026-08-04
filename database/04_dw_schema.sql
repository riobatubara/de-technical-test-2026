-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL
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
    gross_amount NUMERIC(12,2) NOT NULL, -- quantity × unit_price
    
    net_amount NUMERIC(12,2) NOT NULL, -- gross − discount

    tax_amount DECIMAL(12,2), -- for finance
    total_amount DECIMAL(12,2), -- net + tax

    cost_amount DECIMAL(12,2), -- product cost
    profit_amount DECIMAL(12,2), -- net − cost

    item_count INT, -- basket size
    avg_item_price DECIMAL(10,2), -- pricing behavior

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