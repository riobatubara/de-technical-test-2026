-- Seed Customers
INSERT INTO customers (name, email, city, signup_date)
SELECT 
    name,
    lower(replace(name,' ','') || i || '@mail.com'),
    city,
    CURRENT_DATE - (random()*365)::int
FROM (
    SELECT 
        (ARRAY['Ahmad','Budi','Citra','Dewi','Eka','Fajar','Gita','Hadi','Indra','Joko'])[ceil(random()*10)] || ' ' ||
        (ARRAY['Saputra','Wijaya','Santoso','Putri','Lestari','Pratama','Sari','Hidayat','Utama','Kusuma'])[ceil(random()*10)] AS name,
        (ARRAY['Jakarta','Bandung','Surabaya','Medan','Bekasi','Depok','Semarang','Makassar','Bogor','Yogyakarta'])[ceil(random()*10)] AS city,
        generate_series(1,200) i
) t;

-- Seed Products
INSERT INTO products (product_name, category, price)
SELECT 
    product_name,
    category,
    CASE 
        WHEN category = 'Electronics' THEN ROUND((random()*500+100)::numeric,2)
        WHEN category = 'Clothing' THEN ROUND((random()*50+10)::numeric,2)
        WHEN category = 'Food' THEN ROUND((random()*20+2)::numeric,2)
        WHEN category = 'Books' THEN ROUND((random()*30+5)::numeric,2)
    END
FROM (
    SELECT 
        i,
        category,
        (
            CASE category
                WHEN 'Electronics' THEN (ARRAY['Smartphone','Laptop','Headphones','Smart TV'])[ceil(random()*4)]
                WHEN 'Clothing' THEN (ARRAY['T-shirt','Jeans','Jacket','Shoes'])[ceil(random()*4)]
                WHEN 'Food' THEN (ARRAY['Fruits','Bread','Snacks','Beverages'])[ceil(random()*4)]
                WHEN 'Books' THEN (ARRAY['Novel','Cookbook','Biography','Science Book'])[ceil(random()*4)]
            END || ' ' || i
        ) AS product_name
    FROM (
        SELECT 
            generate_series(1,200) i,
            (ARRAY['Electronics','Clothing','Food','Books'])[ceil(random()*4)] AS category
    ) t1
) t2;

-- Seed Transactions
INSERT INTO transactions (customer_id, transaction_date, total_amount)
SELECT 
    customer_id,
    signup_date + cumulative_days::int AS transaction_date,
    0
FROM (
    SELECT 
        c.customer_id,
        c.signup_date,
        
        SUM(
            CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM marketing_campaigns mc
                    WHERE (c.signup_date + running_day::int)
                          BETWEEN mc.start_date AND mc.end_date
                )
                THEN gap_days * 0.5
                ELSE gap_days
            END
        ) OVER (
            PARTITION BY c.customer_id 
            ORDER BY seq
        ) AS cumulative_days

    FROM customers c

    JOIN LATERAL (
        SELECT 
            seq,

            -- define running_day (cumulative timeline)
            SUM(gap_days) OVER (ORDER BY seq) AS running_day,

            gap_days
        FROM (
            SELECT 
                seq,
                CASE 
                    WHEN c.customer_id <= 20 THEN (random()*7 + 3)::int
                    WHEN c.customer_id <= 30 THEN (random()*20 + 10)::int
                    ELSE (random()*30 + 15)::int
                END AS gap_days
            FROM generate_series(
                1,
                CASE 
                    WHEN c.customer_id <= 20 THEN 15
                    WHEN c.customer_id <= 30 THEN 8
                    ELSE 4
                END
            ) AS seq
        ) base
    ) t ON true
) final
WHERE signup_date + cumulative_days::int <= CURRENT_DATE;

-- Seed Transaction Items
INSERT INTO transaction_items (transaction_id, product_id, quantity, price)
SELECT 
    tr.transaction_id,
    p.product_id,
    
    -- high-value buys more quantity
    CASE 
        WHEN tr.customer_id <= 30 THEN (random()*5+2)::int
        ELSE (random()*3+1)::int
    END,

    p.price
FROM transactions tr
JOIN LATERAL (
    SELECT product_id, price
    FROM products
    ORDER BY random()
    LIMIT 
        CASE 
            WHEN tr.customer_id <= 20 THEN (random()*4+2)::int  -- loyal = more items
            ELSE (random()*3+1)::int
        END
) p ON true;

-- Update totals transaction to match items
UPDATE transactions t
SET total_amount = sub.total
FROM (
    SELECT transaction_id, SUM(quantity * price) AS total
    FROM transaction_items
    GROUP BY transaction_id
) sub
WHERE t.transaction_id = sub.transaction_id;

-- Boost transation item for campaign
UPDATE transaction_items ti
SET quantity = quantity + 1
FROM transactions tr
WHERE ti.transaction_id = tr.transaction_id
AND EXISTS (
    SELECT 1
    FROM marketing_campaigns mc
    WHERE tr.transaction_date BETWEEN mc.start_date AND mc.end_date
);

-- Seed Marketing Campaigns
INSERT INTO marketing_campaigns (campaign_name, start_date, end_date, channel)
SELECT 
    'Campaign ' || i,
    start_date,
    start_date + (random()*20 + 7)::int,
    (ARRAY['Email','Social Media','Google Ads','Influencer'])[ceil(random()*4)]
FROM (
    SELECT 
        i,
        DATE '2025-01-01' + (random()*300)::int AS start_date
    FROM generate_series(1,50) i
) t;