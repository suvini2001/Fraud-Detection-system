"""
Fraud SCORING consumer (consumer group "fraud-scorer").

    transactions.raw ──► [ scorer instance 1..N ] ──► transactions.scored
                                   │                └─► fraud.alerts (fraud only)
                                   └──────────────────► transactions.dlq (bad messages)

Kafka features shown here
-------------------------
* Consumer group + cooperative-sticky rebalancing: run N replicas, Kafka
  splits the 6 partitions between them; add/remove replicas at runtime
  (dynamic membership) and partitions move automatically.
* EXACTLY-ONCE processing (PROCESSING_GUARANTEE=exactly_once):
  output messages AND the input offsets are committed in ONE Kafka
  transaction. Either both happen or neither -> no lost and no duplicate
  scores even if the scorer crashes mid-batch.
* AT-LEAST-ONCE fallback (PROCESSING_GUARANTEE=at_least_once):
  produce outputs -> flush -> commit offsets manually.
* Dead Letter Queue: poison messages (bad JSON / schema) are sent to
  transactions.dlq with error details instead of blocking the partition.
* Graceful shutdown: finish batch, commit, close() -> leave group immediately.
"""

import logging
import socket
import time
import uuid
from typing import Dict, List, Optional, Tuple

from confluent_kafka import KafkaError, KafkaException, TopicPartition

from app.common.config import MODEL_DIR, KafkaSettings, ScorerSettings
from app.common.schemas import DeadLetter, ScoredTransaction, TransactionEvent
from app.common.serde import (
    DeserializationError,
    build_headers,
    deserialize_value,
    serialize_key,
    serialize_value,
)

log = logging.getLogger("scorer")

# (topic, key, value, headers)
OutRecord = Tuple[str, Optional[bytes], bytes, list]


class FraudScoringService:
    def __init__(self, kafka: KafkaSettings, settings: ScorerSettings, scorer, consumer, producer):
        self.kafka = kafka
        self.topics = kafka.topics
        self.settings = settings
        self.scorer = scorer
        self.consumer = consumer
        self.producer = producer
        self.exactly_once = settings.processing_guarantee == "exactly_once"
        self.stats = {"processed": 0, "fraud": 0, "dlq": 0, "batches": 0, "aborted": 0}

    # ------------------------------------------------------------------ #
    # Rebalance callbacks  (dynamic membership, visible in the logs)
    # ------------------------------------------------------------------ #
    def on_assign(self, consumer, partitions):
        log.info("REBALANCE +assigned %s", _fmt(partitions))

    def on_revoke(self, consumer, partitions):
        # We always commit at the end of each batch (sync), so there is
        # nothing pending here; just log which partitions moved away.
        log.info("REBALANCE -revoked  %s", _fmt(partitions))

    def on_lost(self, consumer, partitions):
        # Partitions were taken away without a clean revoke (e.g. session
        # timeout). Any uncommitted work will be redone by the new owner.
        log.warning("REBALANCE !lost     %s", _fmt(partitions))

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    def start(self):
        if self.exactly_once:
            # Registers transactional.id with the transaction coordinator and
            # FENCES any zombie instance using the same id.
            self.producer.init_transactions(30)
        self.consumer.subscribe(
            [self.topics.raw],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_lost,
        )
        log.info("Subscribed to '%s' as group '%s' (%s)",
                 self.topics.raw, self.settings.group_id, self.settings.processing_guarantee)

    def poll_once(self) -> int:
        msgs = self.consumer.consume(
            num_messages=self.settings.batch_size, timeout=self.settings.poll_timeout_s
        )
        if not msgs:
            return 0
        good = []
        for m in msgs:
            err = m.error()
            if err is None:
                good.append(m)
            elif err.code() == KafkaError._PARTITION_EOF:
                continue
            elif err.fatal():
                raise KafkaException(err)
            else:
                log.warning("Consumer error (retriable): %s", err)
        if good:
            self.process_batch(good)
        return len(good)

    def run(self, shutdown):
        self.start()
        last_report = time.time()
        try:
            while not shutdown.requested:
                self.poll_once()
                if time.time() - last_report >= 10:
                    log.info("stats %s", self.stats)
                    last_report = time.time()
        finally:
            log.info("Closing consumer (leaving group -> immediate rebalance)...")
            self.consumer.close()
            self.producer.flush(10)
            log.info("Scorer stopped. %s", self.stats)

    # ------------------------------------------------------------------ #
    # Batch processing
    # ------------------------------------------------------------------ #
    def process_batch(self, msgs) -> None:
        outputs: List[OutRecord] = []
        for m in msgs:
            outputs.extend(self.handle_message(m))

        offsets = _next_offsets(msgs)
        if self.exactly_once:
            self._commit_transactional(outputs, offsets, msgs)
        else:
            self._commit_at_least_once(outputs, offsets)
        self.stats["batches"] += 1

    def handle_message(self, msg) -> List[OutRecord]:
        """Turn ONE input message into the output records it should produce."""
        try:
            event = deserialize_value(msg.value(), TransactionEvent)
        except DeserializationError as exc:
            return [self._dead_letter(msg, "DeserializationError", str(exc))]

        try:
            prob, is_fraud = self.scorer.score(event)
        except Exception as exc:  # model/feature failure for THIS message only
            log.exception("Scoring failed for %s", event.transaction_id)
            return [self._dead_letter(msg, type(exc).__name__, str(exc))]

        scored = ScoredTransaction(
            transaction_id=event.transaction_id,
            nameOrig=event.nameOrig,
            nameDest=event.nameDest,
            type=event.type,
            amount=event.amount,
            fraud_probability=round(prob, 6),
            is_fraud=is_fraud,
            threshold=self.settings.fraud_threshold,
            model_version=self.settings.model_version,
            event_time=event.event_time,
            source_partition=msg.partition(),
            source_offset=msg.offset(),
        )
        key = serialize_key(event.nameOrig)          # keep the SAME key downstream
        value = serialize_value(scored)
        headers = build_headers("fraud-scorer", {"trace-id": event.transaction_id})

        self.stats["processed"] += 1
        out = [(self.topics.scored, key, value, headers)]
        if is_fraud:
            self.stats["fraud"] += 1
            log.warning("FRAUD ALERT tx=%s account=%s amount=%.2f p=%.3f (p%d@%d)",
                        event.transaction_id, event.nameOrig, event.amount, prob,
                        msg.partition(), msg.offset())
            out.append((self.topics.alerts, key, value, headers))
        return out

    def _dead_letter(self, msg, error_type: str, error_message: str) -> OutRecord:
        self.stats["dlq"] += 1
        raw = msg.value()
        dl = DeadLetter(
            error_type=error_type,
            error_message=error_message[:1000],
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
            raw_value=raw.decode("utf-8", errors="replace")[:10000] if raw else None,
            consumer_group=self.settings.group_id,
        )
        log.error("-> DLQ %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), error_type)
        headers = build_headers("fraud-scorer", {"error-type": error_type})
        return (self.topics.dlq, msg.key(), serialize_value(dl), headers)

    def _produce_all(self, outputs: List[OutRecord]) -> None:
        for topic, key, value, headers in outputs:
            while True:
                try:
                    self.producer.produce(topic, key=key, value=value, headers=headers)
                    break
                except BufferError:
                    self.producer.poll(0.5)

    # ---------------- exactly-once ----------------
    def _commit_transactional(self, outputs, offsets: List[TopicPartition], msgs) -> None:
        self.producer.begin_transaction()
        try:
            self._produce_all(outputs)
            # The input offsets become part of the SAME transaction
            self.producer.send_offsets_to_transaction(
                offsets, self.consumer.consumer_group_metadata(), 30
            )
            self._commit_with_retry()
        except KafkaException as exc:
            err = exc.args[0]
            if err.txn_requires_abort():
                log.error("Transaction failed, aborting and rewinding batch: %s", err)
                self.producer.abort_transaction(30)
                self.stats["aborted"] += 1
                self._rewind(msgs)
            else:
                # fatal (e.g. fenced by a newer instance) -> crash, let the
                # orchestrator restart us; Kafka guarantees nothing was committed
                raise

    def _commit_with_retry(self, attempts: int = 5) -> None:
        for i in range(attempts):
            try:
                self.producer.commit_transaction(30)
                return
            except KafkaException as exc:
                err = exc.args[0]
                if err.retriable() and i < attempts - 1:
                    log.warning("commit_transaction retriable error, retrying: %s", err)
                    time.sleep(0.5 * (i + 1))
                    continue
                raise

    def _rewind(self, msgs) -> None:
        """Seek back to the first offset of the batch on each partition so the
        aborted messages are consumed and processed again."""
        first: Dict[Tuple[str, int], int] = {}
        for m in msgs:
            k = (m.topic(), m.partition())
            first[k] = min(first.get(k, m.offset()), m.offset())
        for (topic, partition), offset in first.items():
            try:
                self.consumer.seek(TopicPartition(topic, partition, offset))
            except KafkaException as exc:
                # partition may have been revoked meanwhile; new owner
                # resumes from the last committed offset anyway
                log.warning("seek failed for %s[%d]: %s", topic, partition, exc)

    # ---------------- at-least-once ----------------
    def _commit_at_least_once(self, outputs, offsets: List[TopicPartition]) -> None:
        self._produce_all(outputs)
        remaining = self.producer.flush(30)  # wait for acks=all on every output
        if remaining:
            raise RuntimeError(f"{remaining} output messages not delivered; not committing")
        # Only now is it safe to say "these inputs are done"
        self.consumer.commit(offsets=offsets, asynchronous=False)


def _next_offsets(msgs) -> List[TopicPartition]:
    """Offset to commit = last processed offset + 1, per partition."""
    last: Dict[Tuple[str, int], int] = {}
    for m in msgs:
        k = (m.topic(), m.partition())
        last[k] = max(last.get(k, -1), m.offset())
    return [TopicPartition(t, p, o + 1) for (t, p), o in sorted(last.items())]


def _fmt(partitions) -> str:
    return ", ".join(f"{p.topic}[{p.partition}]" for p in partitions) or "(none)"


def main():
    from app.common.kafka_clients import make_consumer, make_producer, wait_for_cluster
    from app.common.logging_setup import GracefulShutdown, setup_logging
    from app.common.model import FraudScorer

    setup_logging("scorer")
    kafka = KafkaSettings()
    settings = ScorerSettings()
    shutdown = GracefulShutdown()

    scorer = FraudScorer(MODEL_DIR, settings.fraud_threshold)
    t = kafka.topics
    wait_for_cluster(kafka, [t.raw, t.scored, t.alerts, t.dlq])

    exactly_once = settings.processing_guarantee == "exactly_once"
    # Unique per instance. Since KIP-447 (Kafka 2.5+) zombie fencing is done
    # through the consumer-group metadata, so it does not need to be stable.
    txn_id = f"fraud-scorer-{socket.gethostname()}-{uuid.uuid4().hex[:8]}" if exactly_once else None

    consumer = make_consumer(kafka, settings.group_id, settings.group_instance_id)
    producer = make_producer(kafka, transactional_id=txn_id)
    FraudScoringService(kafka, settings, scorer, consumer, producer).run(shutdown)


if __name__ == "__main__":
    main()
