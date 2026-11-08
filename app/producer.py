"""
Transaction PRODUCER: simulates a payment gateway publishing transactions.

Kafka features shown here
-------------------------
* Message KEY = sender account (nameOrig)
    -> hash(key) % partitions decides the partition, so every transaction of
       one account lands on the SAME partition, in ORDER. Different accounts
       spread across all partitions -> parallel processing.
* acks=all + idempotence (see common/kafka_clients.py) -> no loss, no duplicates.
* Asynchronous send with a delivery callback (no flush() after every message,
  which the old code did and which kills throughput).
* Headers with schema version / content type / trace id.
* Graceful shutdown: flush() pending messages on SIGTERM.
"""

import random
import time
import uuid

from confluent_kafka import KafkaException

from app.common.config import KafkaSettings, ProducerSettings
from app.common.kafka_clients import make_producer, wait_for_cluster
from app.common.logging_setup import GracefulShutdown, setup_logging
from app.common.schemas import TransactionEvent
from app.common.serde import build_headers, serialize_key, serialize_value

log = setup_logging("producer")

_NORMAL_TYPES = ["PAYMENT", "CASH_IN", "CASH_OUT", "DEBIT", "TRANSFER"]


class TransactionGenerator:
    """Generates PaySim-like transactions from a fixed pool of customers."""

    def __init__(self, num_customers: int, fraud_ratio: float, seed: int = None):
        self.rng = random.Random(seed)
        self.fraud_ratio = fraud_ratio
        self.customers = [f"C{self.rng.randint(100000000, 999999999)}" for _ in range(num_customers)]

    def next(self) -> TransactionEvent:
        rng = self.rng
        name_orig = rng.choice(self.customers)
        step = rng.randint(0, 719)

        if rng.random() < self.fraud_ratio:
            # Fraud pattern from the EDA: TRANSFER / CASH_OUT that drains the account
            tx_type = rng.choice(["TRANSFER", "CASH_OUT"])
            amount = round(rng.uniform(50_000, 500_000), 2)
            old_org, new_org = amount, 0.0
            old_dest, new_dest = 0.0, 0.0
            name_dest = f"C{rng.randint(100000000, 999999999)}"
        else:
            tx_type = rng.choice(_NORMAL_TYPES)
            amount = round(rng.uniform(10, 5_000), 2)
            old_org = round(rng.uniform(5_000, 50_000), 2)
            new_org = round(max(0.0, old_org - amount), 2)
            old_dest = round(rng.uniform(100, 20_000), 2)
            new_dest = round(old_dest + amount, 2)
            prefix = "M" if tx_type == "PAYMENT" else "C"
            name_dest = f"{prefix}{rng.randint(100000000, 999999999)}"

        return TransactionEvent(
            transaction_id=str(uuid.uuid4()),
            step=step,
            type=tx_type,
            amount=amount,
            nameOrig=name_orig,
            oldbalanceOrg=old_org,
            newbalanceOrig=new_org,
            nameDest=name_dest,
            oldbalanceDest=old_dest,
            newbalanceDest=new_dest,
        )


class DeliveryStats:
    def __init__(self):
        self.ok = 0
        self.failed = 0
        self.per_partition = {}

    def callback(self, err, msg):
        """Called by librdkafka (from producer.poll/flush) once the broker
        acknowledged - or finally rejected - a message."""
        if err is not None:
            self.failed += 1
            log.error("Delivery FAILED key=%s: %s", msg.key(), err)
            return
        self.ok += 1
        self.per_partition[msg.partition()] = self.per_partition.get(msg.partition(), 0) + 1
        log.debug("Delivered key=%s -> %s[%d]@%d",
                  msg.key().decode(), msg.topic(), msg.partition(), msg.offset())


def run() -> None:
    kafka = KafkaSettings()
    settings = ProducerSettings()
    shutdown = GracefulShutdown()

    wait_for_cluster(kafka, [kafka.topics.raw])
    producer = make_producer(kafka)
    generator = TransactionGenerator(settings.num_customers, settings.fraud_ratio)
    stats = DeliveryStats()

    interval = 1.0 / settings.tps if settings.tps > 0 else 0
    sent = 0
    last_report = time.time()
    log.info("Producing to '%s' at %.1f tx/s (customers=%d, fraud_ratio=%.2f)",
             kafka.topics.raw, settings.tps, settings.num_customers, settings.fraud_ratio)

    while not shutdown.requested:
        event = generator.next()
        try:
            producer.produce(
                topic=kafka.topics.raw,
                key=serialize_key(event.nameOrig),          # PARTITION KEY
                value=serialize_value(event),               # SERIALIZATION
                headers=build_headers("transaction-simulator",
                                      {"trace-id": event.transaction_id}),
                on_delivery=stats.callback,
            )
            sent += 1
        except BufferError:
            # Local queue full (broker slow/down): let librdkafka drain it, retry
            log.warning("Local producer queue full, waiting...")
            producer.poll(1.0)
            continue
        except KafkaException as exc:
            err = exc.args[0]
            if err.fatal():
                log.critical("Fatal producer error: %s", err)
                break
            log.error("Produce error: %s", err)

        # Serve delivery callbacks (non-blocking)
        producer.poll(0)

        if time.time() - last_report >= 10:
            log.info("sent=%d delivered=%d failed=%d per-partition=%s",
                     sent, stats.ok, stats.failed, dict(sorted(stats.per_partition.items())))
            last_report = time.time()

        if settings.max_messages and sent >= settings.max_messages:
            break
        if interval:
            shutdown.wait(interval)

    log.info("Flushing %d pending messages...", len(producer))
    remaining = producer.flush(30)
    if remaining:
        log.error("%d messages were NOT delivered", remaining)
    log.info("Producer stopped. delivered=%d failed=%d", stats.ok, stats.failed)


if __name__ == "__main__":
    run()
