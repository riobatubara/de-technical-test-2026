import psycopg2
from psycopg2.extras import RealDictCursor
from pkg.config import conf


def get_connection():
    return psycopg2.connect(
        # for local testing
        # host="localhost",
        host=conf.DB_HOST,
        dbname=conf.DB_NAME,
        user=conf.DB_USER,
        password=conf.DB_PASSWORD,
        port=conf.DB_PORT,
    )


def get_cursor(conn):
    return conn.cursor(cursor_factory=RealDictCursor)


def close(conn=None, cursor=None):
    if cursor:
        cursor.close()
    if conn:
        conn.close()