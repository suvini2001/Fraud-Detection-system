"""
Factory functions for Kafka clients (confluent-kafka / librdkafka).

All reliability settings live here so they are consistent across services.
Each setting has a comment explaining WHY it is set.
"""

import logging
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient

from .config import KafkaSettings

log = logging.getLogger(__name__)


def _librdkafka_logger(name: str):
    return logging.getLogger(f"librdkafka.{name}")


def producer_config(kafka: KafkaSettings, transactional_id: Optional[str] = None) -> dict:
    conf = {
        "bootstrap.servers": kafka.bootstrap_servers,
        "client.id": kafka.client_id,

        # ---------- Reliability ----------
        # acks=all: the leader waits until ALL in-sync replicas have the
        # record before acknowledging. With replication.factor=3 and
        # min.insync.replicas=2, an acked message survives a broker crash.
        "acks": "all",
        # Idempotent producer: broker de-duplicates retries using
        # (producer id, sequence number) -> no duplicates, ordering kept
        # per partition even with retries and up to 5 in-flight requests.
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        # Keep retrying transient errors (leader election, broker restart)
        # for up to 2 minutes before reporting a delivery failure.
        "delivery.timeout.ms": 120000,
        "retry.backoff.ms": 100,

        # ---------- Throughput ----------
        # Wait up to 20 ms to fill a batch, then compress the batch.
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "compression.type": "lz4",

        # ---------- Partitioning ----------
        # murmur2 = same hash as the Java client, so Python and Java
        # producers send the same key to the same partition.
        # "_random" = messages WITHOUT a key are spread randomly.
        "partitioner": "murmur2_random",

        "logger": _librdkafka_logger("producer"),
    }
    if transactional_id:
        # Enables Kafka transactions (exactly-once read-process-write).
        conf["transactional.id"] = transactional_id
        conf["transaction.timeout.ms"] = 60000
        # librdkafka rule: message.timeout.ms (= delivery.timeout.ms) must be
        # <= transaction.timeout.ms, otherwise the producer refuses to start.
        conf["delivery.timeout.ms"] = 60000
    return conf


def consumer_config(
    kafka: KafkaSettings,
    group_id: str,
    group_instance_id: str = "",
    read_committed: bool = True,
) -> dict:
    conf = {
        "bootstrap.servers": kafka.bootstrap_servers,
        "client.id": kafka.client_id,

        # ---------- Consumer group ----------
        # All instances with the same group.id SHARE the topic's partitions
        # (each partition -> exactly one instance). Different group.ids each
        # get their OWN full copy of the stream.
        "group.id": group_id,
        # Cooperative (incremental) rebalancing: when a consumer joins/leaves,
        # only the partitions that move are paused - the rest keep flowing.
        "partition.assignment.strategy": "cooperative-sticky",

        # ---------- Dynamic membership / failure detection ----------
        # A consumer that stops heart-beating for 45 s is considered dead
        # and its partitions are reassigned to the remaining members.
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 3000,
        # If processing a batch takes longer than this, the consumer is
        # kicked out of the group (protects against stuck consumers).
        "max.poll.interval.ms": 300000,

        # ---------- Offsets / delivery semantics ----------
        # We commit offsets OURSELVES only after the result is safely written
        # (manual commit or inside a transaction) -> no message loss.
        "enable.auto.commit": False,
        # New group with no committed offset starts from the beginning.
        "auto.offset.reset": "earliest",

        "logger": _librdkafka_logger("consumer"),
    }
    if read_committed:
        # Only read messages from COMMITTED transactions (hides aborted ones).
        conf["isolation.level"] = "read_committed"
    if group_instance_id:
        # Static membership (KIP-345): a restart with the same id within
        # session.timeout.ms does NOT trigger a rebalance.
        conf["group.instance.id"] = group_instance_id
    return conf


def wait_for_cluster(kafka: KafkaSettings, required_topics, timeout_s: int = 120) -> None:
    """Block until the brokers are reachable and the required topics exist."""
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})
    deadline = time.time() + timeout_s
    while True:
        try:
            md = admin.list_topics(timeout=5)
            missing = [t for t in required_topics if t not in md.topics]
            if not missing:
                log.info(
                    "Kafka cluster ready: %d brokers, topics %s",
                    len(md.brokers), ", ".join(required_topics),
                )
                return
            log.info("Waiting for topics: %s", missing)
        except KafkaException as exc:
            log.info("Waiting for Kafka cluster: %s", exc)
        if time.time() > deadline:
            raise RuntimeError(f"Kafka not ready after {timeout_s}s")
        time.sleep(3)


def make_producer(kafka: KafkaSettings, transactional_id: Optional[str] = None) -> Producer:
    return Producer(producer_config(kafka, transactional_id))


def make_consumer(kafka: KafkaSettings, group_id: str, group_instance_id: str = "",
                  read_committed: bool = True) -> Consumer:
    return Consumer(consumer_config(kafka, group_id, group_instance_id, read_committed))
