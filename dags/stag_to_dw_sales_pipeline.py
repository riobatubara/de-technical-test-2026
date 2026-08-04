import os
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration
STAGING_DIR = "/tmp/etl_staging"
os.makedirs(STAGING_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract() -> list:
    """Extract staging tables to individual CSV files."""
    logger.info("Extract.")
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        tables = ['customers', 'products', 'transactions', 'transaction_items', 'marketing_campaigns']
        saved_files = []
        
        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table};", con=engine)
            file_path = os.path.join(STAGING_DIR, f"{table}.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"Extracted {len(df)} rows to {file_path}")
            saved_files.append(file_path)
            
        return saved_files
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise

def transform_and_load_dw(**kwargs):
    """Parses extracted data, executes star schema normalization, and loads data into DW."""
    logger.info("Starting Phase 2 & 3: Deep Transformation and DW Target Loading.")
    ti = kwargs['ti']
    
    try:
        # Pull payload from XCom
        raw_payload = ti.xcom_pull(task_ids='extract_staging_data_task')
        if not raw_payload:
            raise ValueError("No data retrieved from extraction task XCom channel.")
            
        # Reconstruct DataFrames
        df_cust = pd.read_json(raw_payload['customers'], orient='split')
        df_prod = pd.read_json(raw_payload['products'], orient='split')
        df_tx = pd.read_json(raw_payload['transactions'], orient='split')
        df_tx_items = pd.read_json(raw_payload['transaction_items'], orient='split')
        df_camp = pd.read_json(raw_payload['marketing_campaigns'], orient='split')
        
        # Enforce Explicit Date Types
        df_cust['signup_date'] = pd.to_datetime(df_cust['signup_date'])
        df_tx['transaction_date'] = pd.to_datetime(df_tx['transaction_date'])
        df_camp['start_date'] = pd.to_datetime(df_camp['start_date'])
        df_camp['end_date'] = pd.to_datetime(df_camp['end_date'])
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # =====================================================================
        # 1. TRANSFORM & LOAD: dim_date
        # =====================================================================
        logger.info("Transforming dim_date pipeline rows.")
        min_date = df_tx['transaction_date'].min()
        max_date = df_tx['transaction_date'].max()
        
        if pd.isnull(min_date) or pd.isnull(max_date):
            date_range = pd.date_range(start="2025-01-01", end="2025-12-31")
        else:
            date_range = pd.date_range(start=min_date - timedelta(days=5), end=max_date + timedelta(days=5))
            
        df_dim_date = pd.DataFrame({'full_date': date_range})
        df_dim_date['date_key'] = df_dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
        df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.dayofweek + 1
        df_dim_date['day_name'] = df_dim_date['full_date'].dt.day_name()
        df_dim_date['day_of_month'] = df_dim_date['full_date'].dt.day
        df_dim_date['month_number'] = df_dim_date['full_date'].dt.month
        df_dim_date['month_name'] = df_dim_date['full_date'].dt.month_name()
        df_dim_date['quarter'] = df_dim_date['full_date'].dt.quarter
        df_dim_date['year'] = df_dim_date['full_date'].dt.year
        df_dim_date['is_weekend'] = df_dim_date['day_of_week'].isin([6, 7])
        
        date_records = list(df_dim_date[['date_key', 'full_date', 'day_of_week', 'day_name', 'day_of_month', 
                                         'month_number', 'month_name', 'quarter', 'year', 'is_weekend']].itertuples(index=False, name=None))
        
        execute_values(cursor, """
            INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month, month_number, month_name, quarter, year, is_weekend)
            VALUES %s ON CONFLICT (date_key) DO NOTHING;
        """, date_records)

        # =====================================================================
        # 2. TRANSFORM & LOAD: dim_customer, dim_product, dim_campaign
        # =====================================================================
        logger.info("Transforming core dimensions.")
        cust_records = list(df_cust[['customer_id', 'name', 'email', 'city', 'signup_date']].itertuples(index=False, name=None))
        execute_values(cursor, "INSERT INTO dim_customer (customer_id, name, email, city, signup_date) VALUES %s ON CONFLICT DO NOTHING;", cust_records)
        
        prod_records = list(df_prod[['product_id', 'product_name', 'category', 'price']].itertuples(index=False, name=None))
        execute_values(cursor, "INSERT INTO dim_product (product_id, product_name, category, price) VALUES %s ON CONFLICT DO NOTHING;", prod_records)

        camp_records = list(df_camp[['campaign_id', 'campaign_name', 'start_date', 'end_date', 'channel']].itertuples(index=False, name=None))
        execute_values(cursor, "INSERT INTO dim_campaign (campaign_id, campaign_name, start_date, end_date, channel) VALUES %s ON CONFLICT DO NOTHING;", camp_records)
        
        conn.commit()

        # =====================================================================
        # 3. TRANSFORM & LOAD: fact_sales (Granular Analysis & Key Resolution)
        # =====================================================================
        logger.info("Executing complex joins to generate analytical fact_sales matrix.")
        engine = hook.get_sqlalchemy_engine()
        dim_cust_map = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer", con=engine)
        dim_prod_map = pd.read_sql("SELECT product_key, product_id FROM dim_product", con=engine)
        dim_camp_map = pd.read_sql("SELECT campaign_key, start_date, end_date FROM dim_campaign", con=engine)
        dim_camp_map['start_date'] = pd.to_datetime(dim_camp_map['start_date'])
        dim_camp_map['end_date'] = pd.to_datetime(dim_camp_map['end_date'])

        # Join transaction data together
        df_fact = df_tx_items.merge(df_tx, on='transaction_id', how='inner')
        df_fact = df_fact.merge(dim_cust_map, on='customer_id', how='inner')
        df_fact = df_fact.merge(dim_prod_map, on='product_id', how='inner')
        
        df_fact['transaction_date_key'] = df_fact['transaction_date'].dt.strftime('%Y%m%d').astype(int)
        df_fact['gross_amount'] = df_fact['quantity'] * df_fact['price_x']
        df_fact = df_fact.rename(columns={'price_x': 'unit_price', 'total_amount': 'allocated_total_amount'})

        # Match transaction dates to campaign windows
        def attribute_campaign(row):
            tx_dt = row['transaction_date']
            match = dim_camp_map[(tx_dt >= dim_camp_map['start_date']) & (tx_dt <= dim_camp_map['end_date'])]
            return int(match.iloc[0]['campaign_key']) if not match.empty else None

        df_fact['campaign_key'] = df_fact.apply(attribute_campaign, axis=1)
        df_fact['campaign_key'] = df_fact['campaign_key'].where(df_fact['campaign_key'].notna(), None)

        fact_records = list(df_fact[['transaction_date_key', 'customer_key', 'product_key', 'campaign_key',
                                     'transaction_id', 'transaction_item_id', 'quantity', 'unit_price',
                                     'gross_amount', 'allocated_total_amount']].itertuples(index=False, name=None))
        
        cursor.execute("TRUNCATE TABLE fact_sales RESTART IDENTITY;")
        execute_values(cursor, """
            INSERT INTO fact_sales (transaction_date_key, customer_key, product_key, campaign_key, 
                                    transaction_id, transaction_item_id, quantity, unit_price, gross_amount, allocated_total_amount)
            VALUES %s;
        """, fact_records)
        
        conn.commit()
        logger.info(f"Pipeline executed successfully. Total loaded rows: {len(fact_records)}")
        
    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
        logger.error(f"Critical error caught during process layer: {str(e)}")
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# DAG Workflow
with DAG(
    'stag_to_dw_sales_pipeline',
    default_args=default_args,
    description='ETL orchestration to data warehouse.',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(task_id='extract_staging_data_task', python_callable=extract_staging_data)
    transform_load_task = PythonOperator(task_id='transform_and_load_dw_task', python_callable=transform_and_load_dw)

    extract_task >> transform_load_task
