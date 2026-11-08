# Real-Time Fraud Detection Pipeline

An event-driven fraud-detection system. An XGBoost model scores payment transactions as they stream through a fault-tolerant **Apache Kafka** cluster. Suspected fraud is routed to an alerts topic, and bad messages are isolated in a dead-letter queue. A **FastAPI** gateway lets you score a single transaction or publish one into the stream.

The whole stack runs locally with one `docker compose up`.

> **Status:** ongoing personal project. Planned next steps are listed in the [Roadmap](#roadmap).

---

## Architecture

```mermaid
flowchart LR
    P[Producer<br/>simulated payment gateway] -->|key = sender account| RAW[(transactions.raw<br/>6 partitions, RF=3)]
    API[FastAPI<br/>POST /v1/transactions] -->|acks=all| RAW
    RAW --> S[Scorer x3<br/>group: fraud-scorer<br/>XGBoost]
    S -->|every result| SC[(transactions.scored)]
    S -->|fraud only| AL[(fraud.alerts)]
    S -->|poison messages| DLQ[(transactions.dlq)]
    AL --> N[Alert notifier<br/>group: fraud-alert-notifier]
    N --> F["data/alerts.jsonl"]
    API2[FastAPI<br/>POST /predict] -.->|direct scoring| M[XGBoost model]
```

All four Python services (`api`, `producer`, `scorer`, `alerts`) are built from **one Docker image** and share code in `app/common/`.

---

## Key features

### Reliable Kafka cluster
- **3-broker cluster in KRaft mode** (no ZooKeeper). Each node is both broker and controller.
- **Replication factor 3, `min.insync.replicas=2`**, and unclean leader election disabled. A broker can fail without losing acknowledged messages (try it with `./scripts/demo.sh broker-down`).
- **Idempotent producer with `acks=all`**, so retries never create duplicates.
- Topics are created explicitly with fixed partitions and retention. Auto-creation is turned off.

### Ordering and scaling
- Messages are **keyed by sender account** (`nameOrig`, murmur2 partitioner), so each account's transactions stay in order on one partition.
- **6 partitions** let the scorer consumer group scale horizontally (`--scale scorer=6`), using **cooperative-sticky rebalancing** so only the partitions that move are paused.
- A **separate alert consumer group** reads `fraud.alerts` independently (publish/subscribe).

### Processing guarantees
- **Exactly-once scoring with Kafka transactions.** Output messages and input offsets are committed together (`send_offsets_to_transaction`), and downstream consumers use `read_committed`. If a batch fails, the transaction is aborted and the batch is re-read.
- An **at-least-once mode** is available as a fallback (`PROCESSING_GUARANTEE=at_least_once`).
- The alert notifier is at-least-once and **de-duplicates by `transaction_id`** (idempotent consumer).
- **Dead-letter queue.** Invalid JSON or schema violations go to `transactions.dlq` with the error and source partition/offset, instead of blocking the partition.

### Data contracts and serving
- **Versioned Pydantic schemas** (`schema_version`) with field validation, plus Kafka headers carrying schema version, content type and producer name.
- **Shared feature engineering** for training-style features: log amount, balance deltas, time of day, sender/receiver frequency maps, merchant flag, and one-hot transaction type.
- **Fixed a training–serving skew bug.** The old code used `pd.get_dummies(..., drop_first=True)` on a single row, which dropped the transaction type entirely. The fix and its regression test are in `features.py` and `tests/test_features_serde.py`.

---

## Kafka topics

| Topic | Partitions | Retention | Contents |
| --- | --- | --- | --- |
| `transactions.raw` | 6 | 7 days | Incoming transactions, keyed by sender account |
| `transactions.scored` | 6 | 7 days | Every transaction with its fraud probability and label |
| `fraud.alerts` | 3 | 30 days | Transactions flagged as fraud |
| `transactions.dlq` | 3 | 14 days | Messages that could not be processed, with error details |

All topics use replication factor 3 and `min.insync.replicas=2` (see `scripts/create_topics.sh`).

---


## Roadmap

Planned extensions, not yet implemented:

- [ ] **Online feature store.** Feast with Redis for streaming per-account velocity features (for example, transaction count and amount over the last hour).
- [ ] **Model registry.** MLflow for versioned models, replacing the git-ignored `.pkl` files and letting the scorer load a model by version.
- [ ] **Kubernetes deployment.** Run Kafka with the Strimzi operator and the services as Deployments, with stable consumer identities for static membership.
- [ ] **Monitoring.** Prometheus and Grafana for throughput, consumer lag and latency, and Evidently for data and prediction drift.

---

## Tech stack

Python 3.11 · Apache Kafka 3.x (Confluent Platform 7.6, KRaft) · confluent-kafka · XGBoost · pandas · FastAPI · Pydantic v2 · Docker Compose · pytest

