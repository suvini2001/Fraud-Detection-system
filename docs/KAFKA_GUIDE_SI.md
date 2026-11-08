
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

## Verified run

Results from running the full stack locally (Docker Desktop on Windows, 3 brokers, 3 scorers).

### Results summary

| # | Result | Evidence | Why it holds |
| --- | --- | --- | --- |
| 1 | **Every partition is fully replicated** | Kafka UI: 3 brokers, 204/204 in-sync replicas, later 354/354; 0 under-replicated, 0 out-of-sync | All topics, including the internal `__consumer_offsets` and `__transaction_state`, use replication factor 3, so each partition has a copy on every broker |
| 2 | **Load is spread evenly across brokers** | Leaders per broker: 23 / 23 / 22 out of 68 partitions | Kafka assigns partition leadership round-robin, so no single broker handles all reads and writes |
| 3 | **Scoring scales horizontally** | `fraud-scorer` group: 3 members; logs show partitions 2 per scorer | One partition goes to one member of a group, so 6 partitions let up to 6 scorers work in parallel |
| 4 | **Scorers keep up with the stream** | 13,762 transactions in `transactions.raw`; `fraud-scorer` lag 3 | Lag (messages not yet processed) stays near zero, so batches of 200 are scored faster than the producer sends |
| 5 | **Exactly-once commits are active** | Scorer stats `aborted: 0`; `__transaction_state` topic created; `transactions.scored` shows more offsets (14,437) than `raw` (13,762) | Each batch commits outputs and offsets in one transaction; the extra offsets are the transaction commit markers |
| 6 | **No data loss when a broker fails** | Producer `failed=0` while `kafka-2` was down (2,185 → 2,234 delivered); ISR 2 per partition | With RF 3 and `min.insync.replicas=2`, writes with `acks=all` still succeed with 2 brokers, and leaders move to the survivors |
| 7 | **Automatic recovery** | After restart: 354/354 in sync, 0 under-replicated | The returning broker copies what it missed from the leaders and rejoins the ISR with no manual work |
| 8 | **Per-account ordering** | Same account → same partition, increasing offsets (API demo and scorer logs) | Messages are keyed by sender account; murmur2 hashing maps a key to one partition, and Kafka orders each partition |
| 9 | **Bad messages don't block the stream** | DLQ record with error, source partition and offset; scoring continued | Invalid messages are routed to `transactions.dlq` instead of being retried forever on their partition |
| 10 | **Independent consumers (publish/subscribe)** | `fraud-alert-notifier` group: separate, STABLE, lag 3 | A different `group.id` gets its own copy of `fraud.alerts` with its own offsets, without affecting the scorers |
| 11 | **Detection matches the injected fraud rate** | 60 of the first 600 transactions flagged (10%) | The simulated producer makes 10% of transactions fraud-like (`PRODUCER_FRAUD_RATIO=0.10`) |

### Healthy cluster

All 3 brokers online; all 68 partitions online with **204 of 204 replicas in sync** and no under-replicated partitions.

![Kafka UI: 3 brokers, 204 of 204 in-sync replicas](docs/images/kafka-ui-brokers.png)

### Scoring with exactly-once batches

The 3 scorers split the 6 partitions (2 each) and committed their first batches with no aborted transactions and no dead letters:

```
scorer-1  REBALANCE +assigned transactions.raw[0], transactions.raw[3]
scorer-2  REBALANCE +assigned transactions.raw[2], transactions.raw[5]
scorer-3  REBALANCE +assigned transactions.raw[1], transactions.raw[4]

scorer-1  stats {'processed': 200, 'fraud': 17, 'dlq': 0, 'batches': 1, 'aborted': 0}
scorer-2  stats {'processed': 200, 'fraud': 14, 'dlq': 0, 'batches': 1, 'aborted': 0}
scorer-3  stats {'processed': 200, 'fraud': 29, 'dlq': 0, 'batches': 1, 'aborted': 0}
```

60 of the first 600 transactions (10%) were flagged, matching the producer's configured fraud ratio (`PRODUCER_FRAUD_RATIO=0.10`). Each alert logs its source, for example `FRAUD ALERT ... account=C852717984 amount=444668.44 p=1.000 (p2@27)` = partition 2, offset 27.

### Per-account ordering

Every transaction from the same account landed on the same partition, in offset order:

| Account | Partition | Offsets |
| --- | --- | --- |
| `C916327431` | 2 | 65, 83, 180 |
| `C102494739` | 4 | 69, 102, 166 |
| `C686572494` | 4 | 94, 163, 214 |

Publishing three transactions for one account through the API (`./scripts/demo.sh api`) returned **202 Accepted** with the same partition each time. Offset 1895 belongs to a producer message on the same partition in between:

![Three API calls for account C123456789 all land on partition 2](docs/images/demo-api-same-partition.png)

### Broker failure

With `kafka-2` stopped (`./scripts/demo.sh broker-down`), the producer kept delivering with **zero failed messages**: 2,185 to 2,234 delivered within 10 seconds.

![Producer keeps delivering with failed=0 while kafka-2 is down](docs/images/demo-broker-down-producer.png)

While `kafka-2` was down, leadership moved to brokers 1 and 3 and each partition's in-sync set shrank to 2, which still meets `min.insync.replicas=2`:

```
Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1
Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,3
Partition: 2  Leader: 3  Replicas: 2,3,1  Isr: 3,1
Partition: 3  Leader: 1  Replicas: 2,1,3  Isr: 1,3
Partition: 4  Leader: 1  Replicas: 1,3,2  Isr: 1,3
Partition: 5  Leader: 3  Replicas: 3,2,1  Isr: 3,1
```

After `./scripts/demo.sh broker-up`, `kafka-2` caught up and rejoined the ISR: all **354 of 354** replicas are back in sync with no under-replicated partitions. It holds no leaderships yet; Kafka's automatic preferred-leader rebalance (`auto.leader.rebalance.enable`, checked every 5 minutes by default) moves leaders back to it, while brokers 1 and 3 keep serving in the meantime.

![After recovery: 354 of 354 in sync, kafka-2 back but leading 0 partitions](docs/images/kafka-ui-brokers-after-recovery.png)

The count grew from 68 to 118 partitions because the first transactional producer created the internal `__transaction_state` topic (50 partitions); 118 x 3 replicas = 354.

### End state

After the demos, **13,762 transactions** had flowed through `transactions.raw`, every topic was fully replicated (0 out-of-sync replicas), and both consumer groups were **STABLE** and caught up (lag 3 each), with 3 scorer members.

![Topics: 13,762 raw transactions, 0 out-of-sync replicas](docs/images/kafka-ui-topics.png)

![Consumer groups: fraud-scorer with 3 members, both STABLE, lag 3](docs/images/kafka-ui-consumers.png)

> **Reading the counts:** Kafka UI's "Number of messages" is the offset range. Topics written inside transactions (`transactions.scored`, `fraud.alerts`, `transactions.dlq`) also store a commit marker per partition per transaction, and each marker takes an offset. That is why `transactions.scored` (14,437) shows more offsets than `transactions.raw` (13,762), which is written without transactions. `read_committed` consumers never see the markers.

### Dead-letter queue

An invalid message (`./scripts/demo.sh dlq`) was routed to `transactions.dlq` with the validation errors, its source topic, partition and offset, and the consumer group that rejected it. The partition kept flowing:

![Dead letter with error details and source coordinates](docs/images/demo-dlq.png)

---

## Quick start

### Prerequisites
- **Docker Desktop**, running, with at least **6 GB of memory** assigned (Settings → Resources). The 3 brokers use about 400 MB each; the whole stack used about 2.3 GB in testing.
- **Git Bash or WSL** on Windows to run the `scripts/*.sh` helpers.
- The trained model artifacts in `app/model/` (not committed to Git, see note below):
  - `fraud_xgb_model.pkl`
  - `model_features.pkl`
  - `frequency_maps.pkl`

> **Note:** model files are git-ignored to keep the repository small. Moving them to a model registry (MLflow) is on the [Roadmap](#roadmap).

### Run

```bash
docker compose up -d --build
docker compose ps -a
```

Expected: `kafka-1`, `kafka-2` and `kafka-3` are `Up (healthy)`, `kafka-init` is `Exited (0)` (topics created), and `api`, `producer`, `scorer` x3 and `alerts` are `Up`. On a laptop, the scorers can take a minute or two to load the model and initialise their transactions.

| Service | URL |
| --- | --- |
| FastAPI docs (Swagger) | http://localhost:8000/docs |
| Kafka UI | http://localhost:8080 |

Check that scoring is running:

```bash
docker compose logs -f scorer
```

Look for `Subscribed to 'transactions.raw' as group 'fraud-scorer' (exactly_once)`, `REBALANCE +assigned`, `FRAUD ALERT` and `stats {...}` lines.

Stop everything with `docker compose down` (add `-v` to delete Kafka data).

---

## API

### `POST /predict`: score one transaction directly

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
        "step": 10, "type": "TRANSFER", "amount": 181000,
        "nameOrig": "C1305486145", "oldbalanceOrg": 181000, "newbalanceOrig": 0,
        "nameDest": "C553264065", "oldbalanceDest": 0, "newbalanceDest": 0
      }'
```

Returns `transaction_id`, `fraud_probability` and `is_fraud`.

### `POST /v1/transactions`: publish into the Kafka stream

The same request body returns **202 Accepted** once the in-sync replicas have acknowledged the message. The response includes the topic, key, partition and offset. The transaction is then scored by the consumer group.

| Error | Meaning |
| --- | --- |
| 422 | Request body failed schema validation |
| 503 | Producer unavailable or its queue is full |
| 504 | Timed out waiting for the Kafka acknowledgement |
| 502 | Kafka delivery failed |

### `GET /health`
Reports whether Kafka is reachable and how many brokers are visible.

---

## Demo script

`scripts/demo.sh` walks through the Kafka features step by step:

```bash
./scripts/demo.sh topics        # partitions, leaders, replicas, ISR
./scripts/demo.sh groups        # which scorer owns which partition, and lag
./scripts/demo.sh scale 6       # add consumers and watch the rebalance
./scripts/demo.sh broker-down   # stop one broker; writes keep working
./scripts/demo.sh broker-up     # broker rejoins and catches up
./scripts/demo.sh dlq           # send a poison message and see it in the DLQ
./scripts/demo.sh api           # publish through the API; same account -> same partition
./scripts/demo.sh lag           # consumer lag (needs `watch`, not in Git Bash; use `groups`)
```

---

## Configuration

Every service reads the same environment variables (`app/common/config.py`), so the system can be configured from Docker Compose or Kubernetes without code changes. The main ones:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-1:29092,kafka-2:29092,kafka-3:29092` | Brokers to connect to |
| `PROCESSING_GUARANTEE` | `exactly_once` | `exactly_once` or `at_least_once` |
| `FRAUD_THRESHOLD` | `0.5` | Probability above which a transaction is flagged |
| `SCORER_BATCH_SIZE` | `200` | Messages per scoring batch |
| `MODEL_VERSION` | `xgb-v1` | Version tag added to scored messages |
| `PRODUCER_TPS` | `5` | Simulated transactions per second |
| `PRODUCER_FRAUD_RATIO` | `0.10` | Share of simulated transactions that look fraudulent |
| `PRODUCER_NUM_CUSTOMERS` | `500` | Size of the simulated customer pool |
| `GROUP_INSTANCE_ID` | *(empty)* | Optional static membership to avoid rebalances on restart |

---

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

20 unit tests cover:
- the transactional commit path, abort and batch rewind, fatal errors, and retrying `init_transactions` after a timeout;
- at-least-once commit ordering;
- dead-letter contents, serialization and schema validation;
- feature engineering, including the one-hot regression test;
- **real** confluent-kafka producer and consumer construction from our configs, so an invalid setting combination fails in CI instead of in the containers.

Kafka is replaced with fakes (`tests/fakes.py`) for the service tests, so no broker is needed. One test that loads the real model is skipped if the model files are missing.

---


## Tech stack

Python 3.11 · Apache Kafka 3.6 (Confluent Platform 7.6, KRaft) · confluent-kafka · XGBoost · pandas · FastAPI · Pydantic v2 · Docker Compose · pytest

## Author

**Suvini Fonseka**, Computer Engineering, University of Peradeniya · [GitHub](https://github.com/suvini2001) · [LinkedIn](https://www.linkedin.com/in/suvinirfonseka)
