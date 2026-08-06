import logging
import time
import signal
import random
from datetime import datetime, timezone
from collections import defaultdict

from pkg.config import conf
from pkg.db import get_connection, get_cursor, close
from pkg.kafka import create_producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

running = True


def shutdown_handler(sig, frame):
    global running
    logger.info("Shutting down gracefully...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def get_last_transaction_id(cursor):
    cursor.execute("""
        SELECT GREATEST(
            (SELECT COALESCE(MAX(transaction_id), 0) FROM transactions),
            (SELECT COALESCE(MAX(transaction_id), 0) FROM transaction_items)
        ) AS last_id
    """)

    return cursor.fetchone()["last_id"]

def fetch_reference_data(cursor):
    cursor.execute("SELECT customer_id FROM customers")
    customers = [row["customer_id"] for row in cursor.fetchall()]

    cursor.execute("SELECT product_id, price FROM products")
    products = cursor.fetchall()

    if not customers or not products:
        raise Exception("customers/products table is empty")

    return customers, products


def print_completed_aggregation(counts, printed_minutes):
    current_minute = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:00")

    for minute in list(counts.keys()):
        if minute < current_minute and minute not in printed_minutes:
            print(f"{minute} -> {counts[minute]} transactions")
            printed_minutes.add(minute)
            del counts[minute]


def main():
    conn = None
    cursor = None
    producer = None

    counts = defaultdict(int)
    printed_minutes = set()

    try:
        conn = get_connection()
        cursor = get_cursor(conn)

        customers, products = fetch_reference_data(cursor)
        last_transaction_id = get_last_transaction_id(cursor)
        last_transaction_id = get_last_transaction_id(cursor) + 1

        producer = create_producer()

        logger.info("Generator started...")

        while running:
            try:
                customer_id = random.choice(customers)
                product = random.choice(products)

                product_id = product["product_id"]
                price = float(product["price"])
                quantity = random.randint(1, 5)

                now = datetime.now(timezone.utc)

                transaction = {
                    "transaction_id": last_transaction_id,
                    "customer_id": customer_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "price": price,
                    "transaction_date": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "total_amount": round(price * quantity, 2)
                }

                producer.send(conf.KAFKA_TOPIC, transaction)

                last_transaction_id += 1

                # aggregation per minute
                minute_key = now.strftime("%Y-%m-%d %H:%M:00")
                counts[minute_key] += 1

                # print completed minute only
                print_completed_aggregation(
                    counts,
                    printed_minutes
                )

                time.sleep(1)

            except Exception as e:
                logger.error(f"Loop error: {str(e)}")
                last_transaction_id -= 1
                time.sleep(2)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")

    finally:
        logger.info("Cleaning up...")
        if producer:
            producer.flush()
            producer.close()

        close(conn, cursor)


if __name__ == "__main__":
    main()