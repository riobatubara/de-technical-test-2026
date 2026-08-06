import json
from kafka import KafkaProducer, KafkaConsumer
from app.pkg.config import conf


def create_producer():
    return KafkaProducer(
        bootstrap_servers=conf.KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3
    )


def create_consumer():
    return KafkaConsumer(
        conf.KAFKA_TOPIC,
        bootstrap_servers=conf.KAFKA_BROKERS,
        group_id=conf.KAFKA_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )