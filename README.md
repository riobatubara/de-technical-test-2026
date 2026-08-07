# de-technical-test-2026

<!-- docker exec -it de_tech_test_db psql -U user -d tech_test_db

docker exec -it de_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic stream.transaction.raw

SELECT ti.transaction_item_id, t.transaction_id, ti.product_id, ti.quantity, ti.price, t.total_amount
FROM transactions t
RIGHT JOIN transaction_items ti ON ti.transaction_id = t.transaction_id
LIMIT 4; -->


## Explanation of Modeling Choices
- Star Schema Selection: Make query simple, fast count, and very easy for BI tools.
- Fact Table Granularity: Set at the transaction line item level to allow granular slice-and-dice operations without losing data.
- Surrogate Keys (_key): Use number key, not source ID. Make join fast, safe from system change, and good for future Slowly Changing Dimensions (SCD).
- Dedicated dim_date: Change SQL date to number index (YYYYMMDD). No slow parsing, make season filter fast.
- Degenerate Dimensions: Keep old source ID inside fact table for easy check back to system.
- Measures Strategy: Save already-calculated and checked numbers. No recalculate at runtime, data always same.
- Fact Partitioning: Split table by transaction_date_key. Big table scan become very fast because auto-drop unneeded part.


## ERD Schema
![ERD Schema](./ERD.jpg)

## Architecture
![Architecture Pipeline](./architecture-pipeline.jpeg)

### Explanation Architecture
#### Stream Pipeline
The stream pipeline handles continuous, real-time data ingestion and initial storage.
- Transaction Generator: Simulates/Generates tranasction data. It fetches details from the customers and products tables to build complete transaction contexts.
- Apache Kafka: Acts as the real-time message broker. The generator publishes raw transactions to the topic stream.transaction.raw.
- Processor: Consumes the messages from Kafka under the consumer group transaction_group. It processes the stream and inserts the records into databases: transactions and transaction_items.

#### ETL Pipeline
The ETL (Extract, Transform, Load) pipeline handles periodic batch processing to move data into the data warehouse.
- Extract: Fetches historical or batched records from all source tables (customers, products, marketing_campaigns, transactions, and transaction_items) and moves them into a temporary Staging Schema.
- Transformation: Reads from the staging schema to clean, structure, and format the data according to data warehouse standards.
- Load & Create Partition: Inserts the processed, structured data into the Data Warehouse (DW).
    - It populates the Dimension Tables (dim_customer, dim_product, dim_date, dim_campaign).
    - It creates specific data partitions before inserting the transactional metrics into the Fact Table (fact_sales) to optimize analytical query speeds.

<!-- 1. DAG Trigger (Daily)
- Runs once per day
- Starts from start_date = 2025-01-01

2. Task 1: Extract (extract_to_staging)
- Connect to Postgres
- Stored into staging tables:
    stag_customers
    stag_products
    stag_transactions
    stag_transaction_items
    stag_marketing_campaigns
    Extract data from source tables

3. Task 2: Transform & Load (transform_and_load_dw)

A. Read Staging Data
- Load all staging tables into pandas DataFrames
B. Transform
- Clean + validate:
- Transactions (dates, amounts, duplicates)
- Customers (email, city, signup date)
- Products (price, category)
- Campaigns (date logic)
- Create dimensions:
    dim_customer
    dim_product
    dim_campaign
    dim_date
C. Load Dimensions
- Upsert into:
    dim_customer
    dim_product
    dim_campaign
    dim_date
D. Build Fact Table (fact_sales)
- Join:
    transactions + items + dimensions
- Calculate metrics:
    gross, net, tax, total
    cost, profit
    item_count, avg_item_price
- Remove duplicates (existing records check)
E. Partition Handling
- Create monthly partitions:
    fact_sales_YYYY_MM
F. Load Fact
Insert into fact_sales -->