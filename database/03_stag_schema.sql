CREATE TABLE IF NOT EXISTS stag_customers (
    customer_id INT,
    name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100),
    signup_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stag_products (
    product_id INT,
    product_name VARCHAR(150),
    category VARCHAR(100),
    price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS stag_transactions (
    transaction_id INT,
    customer_id INT,
    transaction_date TIMESTAMP,
    total_amount DECIMAL(12,2)
);

CREATE TABLE IF NOT EXISTS stag_transaction_items (
    transaction_item_id INT,
    transaction_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS stag_marketing_campaigns (
    campaign_id INT,
    campaign_name VARCHAR(150),
    start_date DATE,
    end_date DATE,
    channel VARCHAR(50)
);