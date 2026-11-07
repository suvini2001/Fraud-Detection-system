"""
Fraud ALERT consumer (consumer group "fraud-alert-notifier").

It reads `fraud.alerts` with a DIFFERENT group.id than the scorer, which
shows the publish/subscribe side of Kafka: any number of independent
groups can read the same topic, each keeping its own offsets.
(In a real system this would send SMS/e-mail, open a case, block a card...)

Semantics: at-least-once. The alert is written first, then the offset is
committed. A crash in between re-delivers the alert, so the notifier
de-duplicates by transaction_id (idempotent consumer).
"""

import json
import os
from collections import OrderedDict

from app.common.config import AlertSettings, KafkaSettings
from app.common.kafka_clients import make_consumer, wait_for_cluster
from app.common.logging_setup import GracefulShutdown, setup_logging
from app.common.schemas import ScoredTransaction
from app.common.serde import DeserializationError, deserialize_value

log = setup_logging("alerts")


class RecentIds:
    """Small LRU set used to drop duplicate alerts after a re-delivery."""

    def __init__(self, capacity: int = 100_000):
        self.capacity = capacity
        self._d = OrderedDict()

    def seen(self, key: str) -> bool:
        if key in self._d:
            self._d.move_to_end(key)
            return True
        self._d[key] = None
        if len(self._d) > self.capacity:
            self._d.popitem(last=False)
        return False


def main():
    kafka = KafkaSettings()
    settings = AlertSettings()
    shutdown = GracefulShutdown()

    wait_for_cluster(kafka, [kafka.topics.alerts])
    consumer = make_consumer(kafka, settings.group_id)
    consumer.subscribe(
        [kafka.topics.alerts],
        on_assign=lambda c, ps: log.info("assigned %s", [p.partition for p in ps]),
        on_revoke=lambda c, ps: log.info("revoked %s", [p.partition for p in ps]),
    )
    os.makedirs(os.path.dirname(settings.output_file) or ".", exist_ok=True)
    dedup = RecentIds()

    try:
        with open(settings.output_file, "a", encoding="utf-8") as out:
            while not shutdown.requested:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("consumer error: %s", msg.error())
                    continue
                try:
                    alert = deserialize_value(msg.value(), ScoredTransaction)
                except DeserializationError as exc:
                    log.error("bad alert at p%d@%d: %s", msg.partition(), msg.offset(), exc)
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                if not dedup.seen(alert.transaction_id):
                    log.warning("🚨 NOTIFY fraud team: account=%s amount=%.2f p=%.3f tx=%s",
                                alert.nameOrig, alert.amount, alert.fraud_probability,
                                alert.transaction_id)
                    out.write(json.dumps(alert.model_dump(mode="json")) + "\n")
                    out.flush()
                # commit only AFTER the side effect is done
                consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        log.info("Alert consumer stopped")


if __name__ == "__main__":
    main()
