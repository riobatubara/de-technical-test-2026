# de-technical-test-2026

docker exec -it de_tech_test_db psql -U user -d tech_test_db



## Explanation of Modeling Choices
Star Schema Selection: Simplifies analytical business queries and delivers faster aggregations (SUM, AVG) compared to highly normalized transactional setups.

Fact Table Granularity: Designed at the transaction item line level. This allows deep slice-and-dice operations by both product categories and customer demographics without losing granular data.

Surrogate Keys (_key): Used instead of operational business IDs (_id) to isolate the Data Warehouse from operational system changes and seamlessly support future Slowly Changing Dimensions (SCD).

Dedicated dim_date: Replaces raw SQL date functions with an integer key index (YYYYMMDD). This avoids slow date-parsing computations during execution and speeds up seasonal and quarterly filtering.

Degenerate Dimensions: Operational transaction_id and transaction_item_id are kept inside the fact table for complete data auditability back to the operational database.



Modeling Choices
1. Star Schema
- Simple joins → fast analytics
- Easy for BI tools
- Denormalized dimensions → fewer joins

2. Surrogate Keys
- customer_key, product_key, etc.
- why:
    - Faster joins (INT)
    - Decouples from source systems
    - Supports history (future SCD2)

3. Date Dimension
- Precomputed fields (month, quarter, etc.)
- Avoids runtime calculations
- Enables fast filtering & grouping

4. Fact Table Design
- Grain = one transaction line item
- Identified by:
```
transaction_id + transaction_item_id
```
- Ensures:
    - No duplicates
    - Accurate aggregation

5. Measures Strategy
- stored:
    - quantity
    - unit_price
    - gross_amount (validated via CHECK)
- why:
    - Performance (no recalculation at query time)
    - Controlled consistency

6. Campaign Handling
- Uses default “No Campaign” row
- Avoids NULL joins
- Keeps queries simpler

7. Partitioning (Fact only)
- Partition by transaction_date_key
- why:
    - Large table → faster scans
    - Prunes partitions automatically