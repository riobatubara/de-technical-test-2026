import logging
import os
import signal
from pkg.kafka import create_consumer
from pkg.db import get_connection

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

running = True


def shutdown(*_):
    global running
    logger.info("Shutting down gracefully...")
    running = False
    os._exit(0) 


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def insert_event(conn, event):
    cur = None
    try:
        cur = conn.cursor()

        # insert transaction
        cur.execute("""
            INSERT INTO transactions (
                transaction_id, customer_id, transaction_date, total_amount
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """, (
            event["transaction_id"],
            event["customer_id"],
            event["transaction_date"],
            event["total_amount"],
        ))

        # insert item
        cur.execute("""
            INSERT INTO transaction_items (
                transaction_id, product_id, quantity, price
            ) VALUES (%s, %s, %s, %s);
        """, (
            event["transaction_id"],
            event["product_id"],
            event["quantity"],
            event["price"],
        ))

        conn.commit()

    except Exception as e:
        logger.error(f"DB error: {str(e)}")
        conn.rollback()

    finally:
        if cur:
            cur.close()


def main():
    consumer = None
    conn = None

    try:
        consumer = create_consumer()
        conn = get_connection()

        logger.info("Processor started. Waiting for messages...")
        
        while running:
            msg_pack = consumer.poll(timeout_ms=1000)

            if not running:
                break

            for _, messages in msg_pack.items():
                for msg in messages:
                    try:
                        event = msg.value
                        insert_event(conn, event)
                    except Exception as e:
                        logger.error(f"Processing error: {str(e)}")

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")

    finally:
        logger.info("Cleaning up...")
        if consumer:
            consumer.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()