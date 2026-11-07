import json
from dataclasses import replace

import pytest
from confluent_kafka import KafkaError, KafkaException

from app.common.config import KafkaSettings, ScorerSettings
from app.common.schemas import TransactionEvent
from app.common.serde import serialize_value
from app.consumer import FraudScoringService
from tests.fakes import FakeConsumer, FakeMessage, FakeProducer, abortable_error

RAW = "transactions.raw"


class FakeScorer:
    """Flags amounts >= 50k as fraud."""
    def score(self, event):
        p = 0.99 if event.amount >= 50_000 else 0.01
        return p, p >= 0.5


def event(tx_id, amount, account="C111"):
    return TransactionEvent(
        transaction_id=tx_id, step=10, type="TRANSFER", amount=amount,
        nameOrig=account, oldbalanceOrg=amount, newbalanceOrig=0,
        nameDest="C222", oldbalanceDest=0, newbalanceDest=0,
    )


def msg(partition, offset, ev=None, raw=None):
    value = raw if raw is not None else serialize_value(ev)
    key = ev.nameOrig.encode() if ev else b"C999"
    return FakeMessage(RAW, partition, offset, value, key)


def make_service(guarantee, consumer, producer):
    settings = replace(ScorerSettings(), processing_guarantee=guarantee)
    kafka = KafkaSettings(bootstrap_servers="fake:9092")
    return FraudScoringService(kafka, settings, FakeScorer(), consumer, producer)


def topics_of(records):
    return [t for t, _, _ in records]


def test_exactly_once_routes_outputs_and_commits_offsets_in_transaction():
    batch = [
        msg(0, 5, event("a", 100.0)),
        msg(0, 6, event("b", 90_000.0)),           # fraud
        msg(3, 41, raw=b"{not json"),               # poison -> DLQ
    ]
    consumer, producer = FakeConsumer([batch]), FakeProducer()
    svc = make_service("exactly_once", consumer, producer)
    svc.start()
    svc.poll_once()

    assert producer.txn_log == ["init", "begin", "commit"]
    assert sorted(topics_of(producer.produced)) == sorted([
        "transactions.scored", "transactions.scored", "fraud.alerts", "transactions.dlq"])
    # offsets committed INSIDE the transaction = last offset + 1 per partition
    assert producer.sent_offsets == [(RAW, 0, 7), (RAW, 3, 42)]
    # consumer never commits by itself in EOS mode
    assert consumer.commits == []
    # same key is kept downstream (ordering per account)
    alert = [r for r in producer.produced if r[0] == "fraud.alerts"][0]
    assert alert[1] == b"C111"
    assert json.loads(alert[2])["is_fraud"] is True


def test_dead_letter_contains_source_coordinates():
    consumer, producer = FakeConsumer([[msg(2, 99, raw=b'{"amount": "abc"}')]]), FakeProducer()
    svc = make_service("exactly_once", consumer, producer)
    svc.start(); svc.poll_once()
    (topic, _, value), = producer.produced
    dl = json.loads(value)
    assert topic == "transactions.dlq"
    assert (dl["source_partition"], dl["source_offset"]) == (2, 99)
    assert dl["error_type"] == "DeserializationError"


def test_failed_transaction_is_aborted_and_batch_rewound():
    batch = [msg(1, 10, event("a", 100.0)), msg(1, 11, event("b", 200.0)),
             msg(4, 3, event("c", 300.0))]
    consumer = FakeConsumer([batch])
    producer = FakeProducer(fail_commit_with=abortable_error())
    svc = make_service("exactly_once", consumer, producer)
    svc.start(); svc.poll_once()

    assert producer.txn_log[-1] == "abort"
    assert producer.produced == []                       # nothing became visible
    assert sorted(consumer.seeks) == [(RAW, 1, 10), (RAW, 4, 3)]  # re-read from batch start
    assert svc.stats["aborted"] == 1


def test_fatal_transaction_error_crashes_service():
    fatal = KafkaError(KafkaError._FENCED, "fenced by newer instance", fatal=True)
    consumer = FakeConsumer([[msg(0, 1, event("a", 1.0))]])
    producer = FakeProducer(fail_commit_with=fatal)
    svc = make_service("exactly_once", consumer, producer)
    svc.start()
    with pytest.raises(KafkaException):
        svc.poll_once()


def test_at_least_once_commits_after_flush():
    consumer = FakeConsumer([[msg(0, 0, event("a", 1.0)), msg(5, 8, event("b", 2.0))]])
    producer = FakeProducer()
    svc = make_service("at_least_once", consumer, producer)
    svc.start(); svc.poll_once()
    (offsets,) = consumer.commits
    assert [(tp.topic, tp.partition, tp.offset) for tp in offsets] == [(RAW, 0, 1), (RAW, 5, 9)]


def test_at_least_once_does_not_commit_if_outputs_not_delivered():
    consumer = FakeConsumer([[msg(0, 0, event("a", 1.0))]])
    producer = FakeProducer(unflushed=1)
    svc = make_service("at_least_once", consumer, producer)
    svc.start()
    with pytest.raises(RuntimeError):
        svc.poll_once()
    assert consumer.commits == []
