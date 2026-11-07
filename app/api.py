"""
FastAPI service.

* POST /predict          -> synchronous scoring (same as before, now shares
                            the fixed feature code with the stream scorer)
* POST /v1/transactions  -> INGESTION GATEWAY: validates the transaction and
                            publishes it to Kafka (key = nameOrig, acks=all).
                            Returns 202 + the partition/offset it was written to.
* GET  /health           -> liveness + Kafka connectivity
"""

import threading
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.common.config import MODEL_DIR, KafkaSettings, ScorerSettings
from app.common.logging_setup import setup_logging
from app.common.model import FraudScorer
from app.common.schemas import TransactionEvent, TxType
from app.common.serde import build_headers, serialize_key, serialize_value

log = setup_logging("api")

kafka = KafkaSettings()
state = {}


class TransactionIn(BaseModel):
    """Public request body (transaction_id is optional: generated if missing)."""
    transaction_id: Optional[str] = None
    step: int = Field(ge=0, le=719)
    type: TxType
    amount: float = Field(ge=0)
    nameOrig: str = Field(pattern=r"^C\d+$")
    oldbalanceOrg: float = Field(ge=0)
    newbalanceOrig: float = Field(ge=0)
    nameDest: str = Field(pattern=r"^[CM]\d+$")
    oldbalanceDest: float = Field(ge=0)
    newbalanceDest: float = Field(ge=0)

    def to_event(self) -> TransactionEvent:
        data = self.model_dump()
        data["transaction_id"] = data["transaction_id"] or str(uuid.uuid4())
        return TransactionEvent(**data)


def _poll_loop(producer, stop: threading.Event):
    # Delivery callbacks run inside poll(); keep serving them in background
    while not stop.is_set():
        producer.poll(0.1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = ScorerSettings()
    state["scorer"] = FraudScorer(MODEL_DIR, settings.fraud_threshold)
    try:
        from app.common.kafka_clients import make_producer
        producer = make_producer(kafka)
        stop = threading.Event()
        thread = threading.Thread(target=_poll_loop, args=(producer, stop), daemon=True)
        thread.start()
        state.update(producer=producer, stop=stop)
        log.info("Kafka producer ready (%s)", kafka.bootstrap_servers)
    except Exception as exc:  # API still serves /predict without Kafka
        log.error("Kafka producer not available: %s", exc)
    yield
    if "producer" in state:
        state["stop"].set()
        state["producer"].flush(10)


app = FastAPI(title="Fraud Detection API", version="2.0", lifespan=lifespan)


@app.post("/predict")
def predict(tx: TransactionIn):
    event = tx.to_event()
    prob, is_fraud = state["scorer"].score(event)
    return {"transaction_id": event.transaction_id,
            "fraud_probability": round(prob, 6), "is_fraud": int(is_fraud)}


@app.post("/v1/transactions", status_code=202)
def ingest(tx: TransactionIn):
    producer = state.get("producer")
    if producer is None:
        raise HTTPException(503, "Kafka producer unavailable")
    event = tx.to_event()

    done = threading.Event()
    result = {}

    def on_delivery(err, msg):
        result["err"] = err
        if err is None:
            result.update(partition=msg.partition(), offset=msg.offset())
        done.set()

    try:
        producer.produce(
            kafka.topics.raw,
            key=serialize_key(event.nameOrig),
            value=serialize_value(event),
            headers=build_headers("fraud-api", {"trace-id": event.transaction_id}),
            on_delivery=on_delivery,
        )
    except BufferError:
        raise HTTPException(503, "Producer queue full, retry later")

    # Wait for acks=all from the in-sync replicas (bounded)
    if not done.wait(10):
        raise HTTPException(504, "Timed out waiting for Kafka acknowledgement")
    if result.get("err") is not None:
        raise HTTPException(502, f"Kafka delivery failed: {result['err']}")

    return {"status": "accepted", "transaction_id": event.transaction_id,
            "topic": kafka.topics.raw, "key": event.nameOrig,
            "partition": result["partition"], "offset": result["offset"]}


@app.get("/health")
def health():
    producer = state.get("producer")
    kafka_ok = False
    brokers = 0
    if producer is not None:
        try:
            md = producer.list_topics(timeout=3)
            brokers = len(md.brokers)
            kafka_ok = kafka.topics.raw in md.topics
        except Exception:
            pass
    return {"status": "ok", "model_loaded": "scorer" in state,
            "kafka_ok": kafka_ok, "brokers": brokers}
