"""Build REAL confluent-kafka clients from our configs (no broker needed).
librdkafka validates the settings in the constructor, so a bad combination
fails here instead of in the running containers."""
from confluent_kafka import Consumer, Producer

from app.common.config import KafkaSettings
from app.common.kafka_clients import consumer_config, producer_config


def test_transactional_producer_config_is_valid():
    Producer(producer_config(KafkaSettings(), transactional_id="test-txn"))


def test_plain_producer_config_is_valid():
    Producer(producer_config(KafkaSettings()))


def test_consumer_config_is_valid():
    Consumer(consumer_config(KafkaSettings(), group_id="test-group"))
