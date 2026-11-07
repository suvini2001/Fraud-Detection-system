# Fraud Detection System: Kafka කොටස Industry-Grade කිරීම (සිංහල මාර්ගෝපදේශය)

මේ document එකේ තියෙන්නේ:
1. කලින් තිබ්බ system එක සහ එහි දුර්වලකම්
2. අලුත් architecture එක
3. ඔයා ඉල්ලපු හැම Kafka concept එකක්ම: ඒක මොකක්ද, අපි මොකක්ද කළේ, code එකේ කොහෙද තියෙන්නේ
4. Run කරන විදිහ සහ demo කරන විදිහ (interview එකකදී පෙන්නන්න)
5. CV bullet points

---

## 1. කලින් තිබ්බ system එක

```
producer.py ──► transactions_topic (partition 1, replica 1) ──► consumer.py (1 instance) ──► print()
```

| ප්‍රශ්නය | ප්‍රතිඵලය |
|---|---|
| Broker එකයි (Zookeeper සමඟ) | ඒ broker එක down වුණොත් මුළු system එකම නවතිනවා |
| Topic එක auto-create, partition 1යි | Consumers කීප දෙනෙක් දැම්මත් වැඩ කරන්නේ එක්කෙනයි. Scale වෙන්නේ නෑ |
| Replication factor 1 | Disk එක නැති වුණොත් data නැති වෙනවා |
| Message key නෑ | එකම account එකේ transactions පිළිවෙළට එනවා කියලා sure නෑ |
| `acks` default, idempotence නෑ | Retry එකකදී duplicate හෝ message නැති වෙන්න පුළුවන් |
| හැම message එකකටම පස්සේ `flush()` | Throughput එක ඉතා අඩුයි |
| `enable_auto_commit=True` | Process කරන්න කලින් commit වුණොත් crash එකකදී message නැති වෙනවා |
| Result එක `print()` විතරයි | වෙන system එකකට fraud alerts ගන්න විදිහක් නෑ |
| Error වුණාම print කරලා අතෑරලා දානවා | Bad messages ගැන track එකක් නෑ |
| **Bug:** `get_dummies(drop_first=True)` row 1කට | Transaction `type` එක model එකට කවදාවත් ගියේ නෑ |

---

## 2. අලුත් architecture එක

```
                                   Kafka Cluster (kafka-1, kafka-2, kafka-3)  KRaft mode
 producer (simulator) ─┐          ┌────────────────────────────────────┐
                       ├─key────► │ transactions.raw    6 partitions   │ ──► scorer-1 ┐
 FastAPI /v1/transactions┘        │                     RF=3           │ ──► scorer-2 ├ group "fraud-scorer"
                                  │                                    │ ──► scorer-3 ┘
                                  │ transactions.scored 6 partitions   │ ◄── (Kafka transaction)
                                  │ fraud.alerts        3 partitions   │ ◄── (fraud only) ──► alerts  group "fraud-alert-notifier"
                                  │ transactions.dlq    3 partitions   │ ◄── (bad messages)
                                  └────────────────────────────────────┘
```

Files:

| File | වැඩේ |
|---|---|
| `docker-compose.yml` | Brokers 3, topic init job, Kafka UI, API, producer, scorer x3, alerts |
| `scripts/create_topics.sh` | Topics 4ක් partitions, replication, retention එක්ක හදනවා |
| `app/common/kafka_clients.py` | Producer/consumer reliability settings ඔක්කොම මෙතන |
| `app/common/schemas.py` | Message contracts (Pydantic) |
| `app/common/serde.py` | Serialization / deserialization + headers |
| `app/common/features.py` | Feature engineering (bug fix එකත් එක්ක), API සහ scorer දෙකම පාවිච්චි කරනවා |
| `app/producer.py` | Keyed, idempotent producer |
| `app/consumer.py` | Fraud scorer: consumer group, exactly-once, DLQ |
| `app/alert_consumer.py` | දෙවෙනි consumer group එක |
| `app/api.py` | `/predict` + `/v1/transactions` (Kafka ingestion) + `/health` |
| `tests/` | Kafka logic එකට unit tests 16ක් |

---

## 3. Kafka concepts එකින් එක

### 3.1 Producers

**මොකක්ද:** Kafka එකට data ලියන application එක.

**අපි කළේ (`app/producer.py`):**
- `kafka-python` වෙනුවට **`confluent-kafka`** (librdkafka, C library එක මත). Industry එකේ වැඩිපුරම පාවිච්චි වෙන්නේ මේක. Idempotence, transactions, cooperative rebalancing ඔක්කොම support කරනවා.
- **Asynchronous send:** `produce()` කරලා message එක local buffer එකට දානවා. librdkafka එක batches හදලා යවනවා. හැම message එකකටම පස්සේ `flush()` කරන්නේ නෑ (පරණ code එකේ ඒක throughput එක මැරුවා).
- **Delivery callback (`on_delivery`):** Broker එක message එක ලැබුණා කියලා confirm කළාම (හෝ fail වුණාම) call වෙනවා. ඒ හරහා partition එකට කීයක් ගියාද කියලා stats print කරනවා.
- `producer.poll(0)`: callbacks run කරන්න.
- `BufferError` (local queue එක full) ආවොත් `poll(1)` කරලා retry කරනවා. Broker එක slow වුණත් producer එක crash වෙන්නේ නෑ (**backpressure**).
- SIGTERM ආවම `flush(30)`: pending messages ඔක්කොම යවලා ඉවර කරලා නවතිනවා (**graceful shutdown**).
- Customer pool එකක් (`PRODUCER_NUM_CUSTOMERS=500`): එකම account එක ආයෙ ආයෙ එනවා, ඒ නිසා per-key ordering demo කරන්න පුළුවන්.

### 3.2 Kafka Cluster

**මොකක්ද:** Brokers (Kafka servers) කිහිපයක් එකට වැඩ කරන එක. Data partitions brokers අතර බෙදිලා, copy වෙලා තියෙනවා.

**අපි කළේ (`docker-compose.yml`):**
- Brokers **3ක්**: `kafka-1`, `kafka-2`, `kafka-3`.
- **KRaft mode:** Zookeeper අයින් කළා. Kafka 3.3+ වල metadata manage කරන්නේ Kafka එකම Raft protocol එකෙන්. Kafka 4.0 වල Zookeeper සම්පූර්ණයෙන්ම අයින් කරලා. ඒ නිසා මේක modern setup එක.
  - `KAFKA_PROCESS_ROLES: broker,controller`: හැම node එකක්ම data store කරනවා (broker) වගේම metadata quorum එකේ ඡන්දය දෙනවා (controller).
  - `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:29093,2@...,3@...`: Controllers 3න් 2ක් ජීවතුන් අතර ඉන්නකම් cluster එක වැඩ.
- Listeners දෙකක්:
  - `INTERNAL://kafka-N:29092`: Docker network එක ඇතුළේ containers සම්බන්ධ වෙන්න.
  - `EXTERNAL://localhost:9092/9094/9096`: ඔයාගේ laptop එකෙන් සම්බන්ධ වෙන්න.
- **Healthcheck** + `kafka-init` job එක: brokers 3ම healthy වුණාට පස්සේ තමයි topics හැදෙන්නේ. ඊට පස්සේ තමයි apps start වෙන්නේ (`service_completed_successfully`). පරණ `depends_on` එකේ race condition එක නැති වුණා.
- Clients වලට brokers **3ම** `bootstrap.servers` එකේ දීලා තියෙනවා. එකක් down වුණත් අනිත් ඒවායින් connect වෙනවා.

### 3.3 Topics

**මොකක්ද:** Messages වර්ග කරලා තියන "log" එකක් (database table එකක් වගේ, හැබැයි append-only).

**අපි කළේ (`scripts/create_topics.sh`):** Auto-create off කරලා (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`) topics 4ක් explicitly හැදුවා:

| Topic | Partitions | Retention | ඇයි |
|---|---|---|---|
| `transactions.raw` | 6 | දින 7 | Input stream එක. Consumers 6 දක්වා parallel |
| `transactions.scored` | 6 | දින 7 | හැම transaction එකකම score එක (analytics, dashboard) |
| `fraud.alerts` | 3 | දින 30 | Fraud විතරයි. ගණන අඩුයි, ඒත් වැඩි කාලයක් තියාගන්නවා |
| `transactions.dlq` | 3 | දින 14 | Process කරන්න බැරි messages පස්සේ බලන්න / replay කරන්න |

Naming convention එක `<domain>.<stage>`. Industry එකේ පාවිච්චි වෙන විදිහ.

### 3.4 Partitioning සහ Partition Key / Message Keys

**මොකක්ද:** Topic එකක් partitions කීපයකට බෙදිලා. **Ordering guarantee එක තියෙන්නේ partition එකක් ඇතුළේ විතරයි.** Message එකට key එකක් දුන්නොත්: `partition = hash(key) % partitions_count`.

**අපි කළේ:**
- Key = **`nameOrig`** (යවන account එක), `producer.py`, `api.py`, `consumer.py` තුනෙම:
  ```python
  producer.produce(topic, key=serialize_key(event.nameOrig), value=..., ...)
  ```
- ඇයි මේ key එක? Fraud detection වලදී **එකම account එකේ transactions පිළිවෙළට** process වෙන්න ඕනේ (උදා: පස්සේ "විනාඩි 5ක් ඇතුළත transactions 10ක්" වගේ per-account features හදනකොට). Accounts ගොඩක් තියෙන නිසා partitions 6ටම load එක සමව බෙදෙනවා.
- Output topics වලත් **එකම key එක** තියාගන්නවා, ඒ නිසා downstream එකේත් ordering එක රැකෙනවා.
- `partitioner: murmur2_random`: librdkafka default එක (crc32) Java client එකට වෙනස්. murmur2 දැම්මම Python/Java producers දෙකම එකම key එක එකම partition එකට යවනවා. Interview එකකදී කියන්න හොඳ detail එකක්.
- **Hot partition** ගැන සැලකිලිමත් වෙන්න: එක account එකකින් අති විශාල traffic එකක් ආවොත් ඒ partition එක විතරක් බර වෙනවා. Account ID වගේ high-cardinality key එකකදී මේක අඩුයි.

### 3.5 Kafka Replication

**මොකක්ද:** හැම partition එකකටම copies (replicas) කිහිපයක් වෙනස් brokers වල. එකක් **leader** (reads/writes), අනිත් ඒවා **followers** (copy කරනවා). Leader එකත් එක්ක up-to-date ඉන්න replicas = **ISR (In-Sync Replicas)**.

**අපි කළේ:**
- `replication.factor=3`: හැම partition එකක්ම brokers 3ටම copy වෙනවා.
- `min.insync.replicas=2`: `acks=all` producer එකක write එකක් සාර්ථක වෙන්න අඩුම තරමින් replicas 2ක ලියවෙන්න ඕනේ.
- `__consumer_offsets`, `__transaction_state` internal topics වලත් RF=3.
- `unclean.leader.election.enable=false`: out-of-sync replica එකක් leader වෙන්න දෙන්නේ නෑ (data loss වළක්වනවා).

**ප්‍රතිඵලය:** Broker 1ක් down වුණත් → ISR = 2 = min.insync → **reads සහ writes දෙකම වැඩ, data loss නෑ.** Brokers 2ක් down වුණොත් writes නවතිනවා (data ආරක්ෂා කරන්න, හිතාමතාම).

### 3.6 Serialization

**මොකක්ද:** Kafka store කරන්නේ bytes විතරයි. Python object → bytes (serialize), bytes → object (deserialize).

**අපි කළේ (`app/common/schemas.py`, `serde.py`):**
- **Pydantic schemas** (message contracts): `TransactionEvent`, `ScoredTransaction`, `DeadLetter`.
  - Types, ranges (`amount >= 0`, `step 0–719`), patterns (`nameOrig` = `C` + digits), `type` = allowed 5න් එකක් විතරයි.
  - `extra="forbid"`: නොදන්නා fields reject කරනවා.
- `schema_version` field එක + header එක: පස්සේ schema එක වෙනස් කළොත් පරණ/අලුත් consumers දෙකටම handle කරන්න පුළුවන් (**schema evolution**).
- **Headers:** `content-type`, `schema-version`, `source`, `trace-id`. Payload එක decode කරන්න කලින්ම metadata බලන්න පුළුවන්. `trace-id` එකෙන් එක transaction එකක් system එක පුරාම trace කරන්න පුළුවන්.
- Deserialize fail වුණොත් `DeserializationError` → **DLQ** එකට (consumer එක crash වෙන්නේ නෑ).
- **ඊළඟ පියවර (optional):** Avro/Protobuf + **Confluent Schema Registry**. Binary නිසා size අඩුයි, compatibility rules broker පැත්තෙන්ම enforce වෙනවා. Interview එකකදී "JSON + Pydantic contracts දැම්මා, Schema Registry එකට migrate කරන්න serde layer එක වෙනම තියෙනවා" කියලා කියන්න පුළුවන්.

### 3.7 Acks සහ Reliability

**`acks` කියන්නේ:** Producer එක "message එක ගියා" කියලා හිතන්නේ කවදාද කියන එක.

| acks | තේරුම | අවදානම |
|---|---|---|
| `0` | යැව්වා, බලන්නේ නෑ | Loss වෙන්න පුළුවන් |
| `1` | Leader ලිව්වාම | Leader crash වුණොත් follower copy කරන්න කලින් loss |
| **`all`** | **ISR ඔක්කොම ලිව්වාම** | **Loss නෑ** (min.insync=2 එක්ක) |

**අපි කළේ (`app/common/kafka_clients.py` → `producer_config`):**
```python
"acks": "all",
"enable.idempotence": True,          # retry එකකදී duplicate නෑ
"max.in.flight.requests.per.connection": 5,
"delivery.timeout.ms": 120000,       # විනාඩි 2ක් දක්වා retry
"linger.ms": 20, "batch.size": 65536, "compression.type": "lz4",   # throughput
```
- **Idempotent producer:** Broker එක හැම producer එකකටම ID එකක් සහ හැම message එකකටම sequence number එකක් බලනවා. Network glitch එකක් නිසා producer එක retry කළත්, broker එක duplicate එක ඉවත් කරනවා. Partition එක ඇතුළේ order එකත් රැකෙනවා.
- Leader election එකකදී (broker down) producer එක auto retry කරනවා. Application code එකට error එකක් එන්නේ නෑ.

### 3.8 Consumers සහ Consumer Groups

**මොකක්ද:**
- **එකම `group.id`** තියෙන consumers → topic එකේ partitions **බෙදාගන්නවා**. එක partition එකක් group එකේ එක consumer කෙනෙක්ට විතරයි. → **load distribution**.
- **වෙනස් `group.id`** → හැම group එකකටම **සම්පූර්ණ copy එකක්**. → **fan-out / pub-sub**.

**අපි කළේ:**
- `fraud-scorer` group: scorer replicas 3, partitions 6 → එක්කෙනෙක්ට partitions 2 බැගින්.
- `fraud-alert-notifier` group: `fraud.alerts` කියවනවා (`app/alert_consumer.py`). Scorer එකට කිසිම බලපෑමක් නැතුව තමන්ගේම offsets එක්ක. පස්සේ "dashboard", "case management" වගේ groups කීපයක් එකතු කරන්න පුළුවන්.

**Offsets / commit (`consumer_config`):**
- `enable.auto.commit=False`: අපි commit කරන්නේ **output එක ආරක්ෂිතව ලිව්වාට පස්සේ** විතරයි.
- `isolation.level=read_committed`: abort වුණු transactions වල messages කියවන්නේ නෑ.

### 3.9 Distribute the workload සහ Scalability

- Partitions 6 = **parallelism එකේ උපරිම සීමාව** (group එකක consumers 6 දක්වා). 7 වෙනි එක idle.
- Runtime එකේ scale කරන්න:
  ```bash
  docker compose up -d --scale scorer=6
  ```
- Model scoring CPU-heavy නිසා consumers වැඩි කළාම throughput එක linear විදිහට වැඩි වෙනවා.
- Batch processing: `consume(num_messages=200)` → batch එකකට එක transaction / එක commit. Overhead අඩුයි.
- **Consumer lag** බලන්න: `./scripts/demo.sh groups`. Lag එක වැඩි වෙනවා නම් scorers වැඩි කරන්න. (Kubernetes වල KEDA එකෙන් lag එක අනුව auto-scale කරන්න පුළුවන්.)
- තවත් scale කරන්න ඕනේ නම් partitions වැඩි කරන්න පුළුවන්. හැබැයි key → partition mapping වෙනස් වෙන නිසා ordering ගැන සැලකිලිමත් වෙන්න. ඒ නිසා මුල ඉඳන්ම partitions ටිකක් වැඩියෙන් තියන එක හොඳයි.

### 3.10 Dynamic Membership (Rebalancing)

**මොකක්ද:** Consumer කෙනෙක් group එකට එකතු වුණාම / ඉවත් වුණාම / crash වුණාම Kafka partitions ආයෙ බෙදනවා (**rebalance**).

**අපි කළේ:**
- `partition.assignment.strategy=cooperative-sticky`: **incremental rebalance**. පරණ "eager" ක්‍රමයේ rebalance එකකදී හැම consumer කෙනෙක්ම නවතිනවා. Cooperative ක්‍රමයේ **ගෙනයන partitions විතරයි** නවතින්නේ. අනිත් ඒවා දිගටම වැඩ.
- `session.timeout.ms=45000`, `heartbeat.interval.ms=3000`: consumer එක තත්පර 45ක් heartbeat නොකළොත් මැරුණා කියලා partitions අනිත් අයට දෙනවා (**failure detection**).
- `max.poll.interval.ms=300000`: process එක හිරවුණු consumer එකක් group එකෙන් අයින් කරනවා.
- `on_assign` / `on_revoke` / `on_lost` callbacks: logs වල `REBALANCE +assigned ...`, `-revoked ...` කියලා පෙනෙනවා. Demo එකේදී පෙන්නන්න හොඳයි.
- **Graceful shutdown:** SIGTERM → batch එක ඉවර කරලා commit කරලා `consumer.close()` → group එකෙන් **වහාම** අයින් වෙනවා (තත්පර 45ක් බලන් ඉන්නේ නෑ).
- **Static membership (optional):** `GROUP_INSTANCE_ID` set කළොත් restart එකකදී (session timeout එක ඇතුළත ආපහු ආවොත්) rebalance එකක් වෙන්නේ නෑ. Kubernetes StatefulSet එකේ pod name එක දෙන්න.

### 3.11 Fault Tolerance (සාරාංශය)

| අසාර්ථකත්වය | සිද්ධ වෙන්නේ |
|---|---|
| Broker 1ක් down | Leaders අනිත් brokers වලට යනවා, ISR=2, producers auto-retry, **data loss නෑ** |
| Scorer instance එකක් crash | Session timeout → rebalance → partitions අනිත් අයට. Commit නොකළ batch එක අලුත් owner ආයෙ process කරනවා. EOS නිසා **duplicate outputs නෑ** |
| Batch එක මැද transaction fail | `abort_transaction()` → outputs කිසිවක් දකින්න නෑ → `seek()` කරලා batch එක ආයෙ කියවනවා |
| Zombie scorer (පරණ instance එක ආයෙ ජීවත් වුණොත්) | Transaction coordinator එක fence කරනවා (KIP-447). Fatal error → crash → restart |
| Bad / corrupt message | DLQ එකට, partition එක block වෙන්නේ නෑ |
| Alert consumer duplicate delivery | `transaction_id` එකෙන් dedup (**idempotent consumer**) |
| Startup order | Healthchecks + kafka-init + `wait_for_cluster()` |

### 3.12 Exactly-Once Semantics (EOS)

මේක CV එකට වැඩිපුරම වටින කොටස.

**ප්‍රශ්නය:** Scorer එක output එක `transactions.scored` එකට ලියලා, offset commit කරන්න කලින් crash වුණොත්? ආයෙ start වුණාම ඒ message එක ආයෙ process කරලා **duplicate score/alert** එකක් යනවා. Offset එක මුලින් commit කරලා output ලියන්න කලින් crash වුණොත් **message එක නැති වෙනවා**.

**විසඳුම: Kafka transactions** (`app/consumer.py` → `_commit_transactional`):
```python
producer.begin_transaction()
for each message: produce(scored / alert / dlq)            # outputs
producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata())  # input offsets
producer.commit_transaction()                               # ALL or NOTHING
```
Outputs සහ input offsets **එකම atomic transaction එකක**. Downstream consumers `read_committed` නිසා commit වුණු දේවල් විතරයි දකින්නේ.

- `transactional.id` හැම instance එකකටම unique.
- `PROCESSING_GUARANTEE=at_least_once` දැම්මොත් සරල mode එක: produce → `flush()` (acks=all ලැබෙනකම්) → `consumer.commit()`. Duplicates වෙන්න පුළුවන්, ඒත් loss නෑ.

### 3.13 Dead Letter Queue (DLQ)

Message එකක් deserialize/validate කරන්න බැරි නම් හෝ scoring fail වුණොත්:
- `transactions.dlq` එකට `DeadLetter` envelope එකක්: error type, message, source topic/partition/offset, original payload.
- Header `error-type`.
- ඒ message එකේ offset එකත් commit වෙනවා. ඒ නිසා එක bad message එකක් මුළු partition එකම block කරන්නේ නෑ (**poison pill** ප්‍රශ්නය).

---

## 4. Run කරන විදිහ

```bash
# 1. Project folder එකට model files දාන්න
#    app/model/fraud_xgb_model.pkl, model_features.pkl, frequency_maps.pkl

# 2.
cp .env.example .env
docker compose up -d --build

# 3. Logs
docker compose logs -f scorer alerts producer

# 4. Kafka UI: http://localhost:8080   (topics, partitions, messages, consumer groups, lag)
#    API docs: http://localhost:8000/docs
```

Tests (Docker නැතුව):
```bash
pip install -r requirements-dev.txt
pytest -q
```

## 5. Demo script (interview / video එකකට)

```bash
./scripts/demo.sh topics        # Partitions 6, Leader, Replicas: 1,2,3, Isr: 1,2,3
./scripts/demo.sh groups        # scorer එක්කෙනෙක්ට partitions 2 බැගින්, LAG
./scripts/demo.sh scale 6       # REBALANCE logs, දැන් partition 1 බැගින්
./scripts/demo.sh broker-down   # kafka-2 නවත්තනවා, Isr: 1,3, producer දිගටම වැඩ
./scripts/demo.sh broker-up     # kafka-2 ආපහු ISR එකට
./scripts/demo.sh dlq           # bad message එකක් DLQ එකට යනවා
./scripts/demo.sh api           # එකම account → එකම partition (response එකේ partition number)
```

---

## 6. Interview එකකදී අහන්න පුළුවන් ප්‍රශ්න (කෙටි උත්තර)

- **ඇයි key එක nameOrig?** Per-account ordering + accounts ගොඩක් නිසා partitions අතර සමව බෙදෙනවා.
- **ඇයි partitions 6?** Consumer parallelism එකේ උපරිමය. Brokers 3ට සමව බෙදෙනවා (2 බැගින් leaders). පස්සේ scale කරන්න ඉඩ.
- **acks=all තිබුණත් data නැති වෙන්න පුළුවන්ද?** `min.insync.replicas=1` නම් ඔව්. ඒකයි අපි 2 දැම්මේ, unclean election off කළේ.
- **Exactly-once කොහොමද?** Idempotent producer + transactions + `send_offsets_to_transaction` + `read_committed`.
- **Consumer එකක් crash වුණොත්?** Heartbeat නැති වෙලා session timeout → cooperative rebalance → partitions අනිත් අයට → last committed offset එකෙන් පටන් ගන්නවා.
- **Cooperative-sticky වල වාසිය?** Rebalance එකකදී stop-the-world නෑ. ගෙනයන partitions විතරයි pause වෙන්නේ.

---

## 7. CV Bullet Points (English)

> **Real-Time Fraud Detection Platform** | Python, Apache Kafka, XGBoost, FastAPI, Docker
> - Built an event-driven fraud detection pipeline on a **3-broker Apache Kafka cluster (KRaft)** with replication factor 3 and `min.insync.replicas=2`, tolerating a broker failure with zero data loss.
> - Implemented **exactly-once** read-process-write using Kafka transactions (`send_offsets_to_transaction`, `read_committed`) and an idempotent `acks=all` producer.
> - Partitioned streams by **account ID** (murmur2 keying) across 6 partitions for per-account ordering and horizontal scaling of a consumer group with **cooperative-sticky rebalancing**.
> - Added schema-validated, versioned message contracts (Pydantic), a **dead letter queue**, graceful shutdown, health-checked provisioning and 16 unit tests covering transaction abort/rewind and commit semantics.
> - Served an XGBoost model (PaySim, 6.3M transactions) via streaming scorers and a FastAPI ingestion gateway; fixed a one-hot encoding training/serving skew bug.
