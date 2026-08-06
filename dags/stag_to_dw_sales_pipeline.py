import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, text, Table, MetaData
from config import conf

from airflow import DAG
from airflow.operators.python import PythonOperator


# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'test_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def ensure_partitions(engine, date_keys, logger):
    if date_keys is None or len(date_keys) == 0:
        logger.warning("No date_keys provided. Skipping partition creation.")
        return

    try:
        months = sorted({str(int(k))[:6] for k in date_keys if pd.notna(k)})

        with engine.begin() as conn:
            for m in months:
                try:
                    year = int(m[:4])
                    month = int(m[4:6])

                    start = f"{year}{month:02d}01"

                    if month == 12:
                        end = f"{year+1}0101"
                    else:
                        end = f"{year}{month+1:02d}01"

                    table_name = f"fact_sales_{year}_{month:02d}"

                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {table_name}
                        PARTITION OF fact_sales
                        FOR VALUES FROM ({start}) TO ({end})
                    """))

                    logger.info(f"Partition ensured: {table_name}")

                except Exception as part_err:
                    logger.error(
                        f"Failed creating partition for {m}: {str(part_err)}"
                    )
                    # continue to next partition instead of failing whole job

    except Exception as e:
        logger.error(f"Critical error in ensure_partitions: {str(e)}")
        raise


def extract_to_staging():

    """Extracts raw datasets from the 5 staging tables"""
    logger.info("Extraction raw datasets.")

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{conf.DB_USER}:{conf.DB_PASSWORD}@{conf.DB_HOST}:{conf.DB_PORT}/{conf.DB_NAME}"
        )
        
        tables = [
            'customers',
            'products',
            'transactions',
            'transaction_items',
            'marketing_campaigns'
        ]
        
        with engine.begin() as conn:
            # Clear previous staging runs safely inside a transaction block
            conn.execute(text("""
                TRUNCATE TABLE
                    stag_customers,
                    stag_products,
                    stag_transactions,
                    stag_transaction_items,
                    stag_marketing_campaigns;
            """))

            for table in tables:
                logger.info(f"Extracting {table} → stag_{table}")

                query = f"SELECT * FROM {table};"

                for chunk in pd.read_sql(query, conn, chunksize=10000):
                    chunk.to_sql(
                        f"stag_{table}",
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )

        logger.info("Extraction completed successfully.")
    except Exception as e:
        logger.error(f"Error encountered during data extraction: {str(e)}")
        raise


def transform_and_load_dw():
    
    try:

        """Reads from staging tables, runs transformation logic, and writes to warehouse."""
        logger.info("Transformation and DW Target Loading.")

        engine = create_engine(
            f"postgresql+psycopg2://{conf.DB_USER}:{conf.DB_PASSWORD}@{conf.DB_HOST}:{conf.DB_PORT}/{conf.DB_NAME}"
        )
        

        # Read staging data
        # =====================================================================
        logger.info("Executing load staging data.")

        df_cust = pd.read_sql("SELECT * FROM stag_customers;", con=engine)
        df_prod = pd.read_sql("SELECT * FROM stag_products;", con=engine)
        df_tx = pd.read_sql("SELECT * FROM stag_transactions;", con=engine)
        df_items = pd.read_sql("SELECT * FROM stag_transaction_items;", con=engine)
        df_camp = pd.read_sql("SELECT * FROM stag_marketing_campaigns;", con=engine)


        # Tranformation
        # =====================================================================
        logger.info("Executing transformation.")

        # transactions transformation
        df_tx['transaction_date'] = pd.to_datetime(df_tx['transaction_date'], errors='coerce')
        df_tx['total_amount'] = pd.to_numeric(df_tx['total_amount'], errors='coerce')

        df_tx = df_tx[
            df_tx['transaction_id'].notna() &
            df_tx['customer_id'].notna() &
            (df_tx['total_amount'] >= 0) &
            (df_tx['transaction_date'] <= pd.Timestamp.today())
        ].drop_duplicates(subset=['transaction_id'])

        # ---- dim_customer
        df_cust['name'] = df_cust['name'].str.strip()
        df_cust['email'] = df_cust['email'].str.strip().str.lower()
        df_cust.loc[df_cust['email'] == '', 'email'] = None
        df_cust['city'] = df_cust['city'].str.strip().str.title()
        df_cust.loc[df_cust['city'] == '', 'city'] = None
        df_cust['signup_date'] = pd.to_datetime(df_cust['signup_date'], errors='coerce')

        df_cust = df_cust[
            df_cust['customer_id'].notna() &
            df_cust['email'].notna() &
            (df_cust['signup_date'] <= pd.Timestamp.today())
        ]

        dim_customer = df_cust.drop_duplicates().copy()
        

        # ---- dim_product
        df_prod['product_name'] = df_prod['product_name'].str.strip()
        df_prod['category'] = df_prod['category'].str.strip().str.title()
        df_prod['price'] = pd.to_numeric(df_prod['price'], errors='coerce')

        df_prod = df_prod[
            df_prod['product_id'].notna() &
            (df_prod['price'] >= 0)
        ]

        dim_product = df_prod.drop_duplicates().copy()
        

        # ---- dim_campaign
        df_camp['campaign_name'] = df_camp['campaign_name'].str.strip()
        df_camp['channel'] = df_camp['channel'].str.strip().str.lower()
        df_camp['start_date'] = pd.to_datetime(df_camp['start_date'], errors='coerce')
        df_camp['end_date'] = pd.to_datetime(df_camp['end_date'], errors='coerce')

        df_camp = df_camp[
            df_camp['campaign_id'].notna() &
            (df_camp['end_date'] >= df_camp['start_date'])
        ]

        dim_campaign = df_camp.drop_duplicates().copy()

        # ensure default campaign_key = 1 exists
        if dim_campaign.empty:
            dim_campaign = pd.DataFrame([{
                "campaign_id": 0,
                "campaign_name": "default",
                "start_date": pd.Timestamp("1900-01-01"),
                "end_date": pd.Timestamp("2099-12-31"),
                "channel": "unknown"
            }])


        # ---- dim_date
        df_dates = pd.DataFrame({
            "full_date": pd.to_datetime(df_tx["transaction_date"]).drop_duplicates()
        })
        df_dates["date_key"] = df_dates["full_date"].dt.strftime("%Y%m%d").astype(int)
        df_dates["day"] = df_dates["full_date"].dt.day
        df_dates["month"] = df_dates["full_date"].dt.month
        df_dates["year"] = df_dates["full_date"].dt.year
        df_dates["quarter"] = df_dates["full_date"].dt.quarter
        dim_date = df_dates
        

        # Load
        # =====================================================================
        logger.info("Executing load dimension tables.")

        meta = MetaData()
        meta.reflect(bind=engine)

        def upsert(df, table_name, keys):
            if df.empty:
                return 0
            table = Table(table_name, meta, autoload_with=engine)
            stmt = insert(table).values(df.to_dict(orient="records"))
            stmt = stmt.on_conflict_do_nothing(index_elements=keys)
            with engine.begin() as conn:
                return conn.execute(stmt).rowcount

        upsert(dim_customer, "dim_customer", ["customer_id"])
        upsert(dim_product,  "dim_product",  ["product_id"])
        upsert(dim_campaign, "dim_campaign", ["campaign_id"])
        upsert(dim_date,     "dim_date",     ["date_key"])


        logger.info("Executing load fact table.")

        # ---- fact_sales
        dim_customer_db = pd.read_sql("SELECT customer_id, customer_key FROM dim_customer", con=engine)
        dim_product_db  = pd.read_sql("SELECT product_id, product_key FROM dim_product", con=engine)
        dim_campaign_db = pd.read_sql("SELECT campaign_id, campaign_key FROM dim_campaign", con=engine)

        fact = df_tx.merge(df_items, on="transaction_id")

        fact = fact.merge(dim_customer_db, on="customer_id")
        fact = fact.merge(dim_product_db,  on="product_id")

        fact = fact.merge(
            dim_date[["full_date", "date_key"]],
            left_on="transaction_date",
            right_on="full_date"
        )

        fact["campaign_key"] = 1

        # metrics
        fact["quantity"] = fact["quantity"]
        fact["unit_price"] = fact["price"]
        fact["gross_amount"] = fact["quantity"] * fact["unit_price"]

        fact["net_amount"] = fact["gross_amount"]
        fact["tax_amount"] = fact["gross_amount"] * 0.10
        fact["total_amount"] = fact["gross_amount"] * 1.10

        fact["cost_amount"] = fact["gross_amount"] * 0.60
        fact["profit_amount"] = fact["gross_amount"] * 0.40

        # window metrics
        fact["item_count"] = fact.groupby("transaction_id")["quantity"].transform("sum")
        fact["avg_item_price"] = fact.groupby("transaction_id")["unit_price"].transform("mean")

        fact_sales = fact[[
            "date_key",
            "customer_key",
            "product_key",
            "campaign_key",
            "transaction_id",
            "transaction_item_id",
            "quantity",
            "unit_price",
            "gross_amount",
            "net_amount",
            "tax_amount",
            "total_amount",
            "cost_amount",
            "profit_amount",
            "item_count",
            "avg_item_price"
        ]].rename(columns={"date_key": "transaction_date_key"})

        existing = pd.read_sql("""
            SELECT transaction_id, transaction_item_id, transaction_date_key
            FROM fact_sales
        """, con=engine)

        if not existing.empty:
            fact_sales = fact_sales.merge(
                existing,
                on=["transaction_id","transaction_item_id","transaction_date_key"],
                how="left",
                indicator=True
            )
            fact_sales = fact_sales[fact_sales["_merge"] == "left_only"].drop(columns=["_merge"])

        if not fact_sales.empty:
            ensure_partitions(
                engine,
                fact_sales["transaction_date_key"].unique(),
                logger
            )
            fact_sales.to_sql("fact_sales", engine, if_exists="append", index=False)

        logger.info(f"Pipeline executed successfully. Total loaded rows: {len(dim_customer) + len(dim_product) + len(dim_campaign) + len(dim_date) + len(fact_sales)}")
        
    except Exception as e:
        logger.error(f"Critical error caught during process layer: {str(e)}")
        raise



# DAG Workflow
with DAG(
    'stag_to_dw_sales_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_to_staging
    )

    transform_and_load_task = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load_dw
    )

    extract_task >> transform_and_load_task