"""In-memory stand-ins for confluent-kafka objects so the processing logic
can be unit-tested without a running broker."""

from confluent_kafka import KafkaError, KafkaException


class FakeMessage:
    def __init__(self, topic, partition, offset, value, key=None, error=None):
        self._t, self._p, self._o, self._v, self._k, self._e = topic, partition, offset, value, key, error

    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return self._v
    def key(self): return self._k
    def error(self): return self._e
    def headers(self): return []


class FakeConsumer:
    def __init__(self, batches=None):
        self.batches = list(batches or [])
        self.commits = []
        self.seeks = []
        self.closed = False
        self.subscribed = None

    def subscribe(self, topics, **callbacks):
        self.subscribed = topics
        self.callbacks = callbacks

    def consume(self, num_messages=1, timeout=1.0):
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets=None, message=None, asynchronous=True):
        self.commits.append(offsets)

    def seek(self, tp):
        self.seeks.append((tp.topic, tp.partition, tp.offset))

    def consumer_group_metadata(self):
        return "group-metadata"

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, fail_commit_with=None, unflushed=0):
        self.produced = []          # committed or not
        self.txn_log = []
        self.sent_offsets = None
        self.fail_commit_with = fail_commit_with
        self.unflushed = unflushed
        self._pending = []

    # transactions
    def init_transactions(self, timeout=None): self.txn_log.append("init")
    def begin_transaction(self): self.txn_log.append("begin"); self._pending = []

    def send_offsets_to_transaction(self, offsets, metadata, timeout=None):
        self.sent_offsets = [(tp.topic, tp.partition, tp.offset) for tp in offsets]

    def commit_transaction(self, timeout=None):
        if self.fail_commit_with is not None:
            err, self.fail_commit_with = self.fail_commit_with, None
            raise KafkaException(err)
        self.txn_log.append("commit")
        self.produced.extend(self._pending)

    def abort_transaction(self, timeout=None):
        self.txn_log.append("abort")
        self._pending = []

    # plain produce
    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        rec = (topic, key, value)
        if "begin" in self.txn_log and self.txn_log[-1] == "begin":
            self._pending.append(rec)
        else:
            self.produced.append(rec)

    def flush(self, timeout=None): return self.unflushed
    def poll(self, timeout=None): return 0


def abortable_error():
    return KafkaError(KafkaError._STATE, "simulated txn failure", txn_requires_abort=True)
