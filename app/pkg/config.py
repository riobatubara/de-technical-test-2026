import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_HOST = os.getenv('DB_HOST', 'postgres')
    DB_NAME = os.getenv('DB_NAME', 'tech_test_db')
    DB_USER = os.getenv('DB_USER', 'user')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'secret')
    DB_PORT = os.getenv('DB_PORT', '5432')

    KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "stream.data.raw")
    KAFKA_GROUP = os.getenv("KAFKA_GROUP_RAW", "data_stream_group")

conf = Config()