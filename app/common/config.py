"""
Central configuration, read from environment variables.

Every service (producer, scorer, alert consumer, API) reads the same names,
so the whole system is configured from docker-compose / Kubernetes without
touching code.
"""

import os
import socket
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Topics:
    # Raw transactions coming in (keyed by sender account = nameOrig)
    raw: str = field(default_factory=lambda: _env("TOPIC_RAW", "transactions.raw"))
    # Every transaction after scoring (probability + label)
    scored: str = field(default_factory=lambda: _env("TOPIC_SCORED", "transactions.scored"))
    # Only the transactions the model flags as fraud
    alerts: str = field(default_factory=lambda: _env("TOPIC_ALERTS", "fraud.alerts"))
    # Messages that could not be processed (bad JSON, schema errors, ...)
    dlq: str = field(default_factory=lambda: _env("TOPIC_DLQ", "transactions.dlq"))


@dataclass(frozen=True)
class KafkaSettings:
    # Give MORE THAN ONE broker: if the first is down the client
    # still bootstraps from the others (fault tolerance).
    bootstrap_servers: str = field(
        default_factory=lambda: _env(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"
        )
    )
    client_id: str = field(default_factory=lambda: _env("KAFKA_CLIENT_ID", socket.gethostname()))
    topics: Topics = field(default_factory=Topics)


@dataclass(frozen=True)
class ScorerSettings:
    group_id: str = field(default_factory=lambda: _env("SCORER_GROUP_ID", "fraud-scorer"))
    # "exactly_once" (Kafka transactions) or "at_least_once" (manual commits)
    processing_guarantee: str = field(
        default_factory=lambda: _env("PROCESSING_GUARANTEE", "exactly_once")
    )
    batch_size: int = field(default_factory=lambda: _env_int("SCORER_BATCH_SIZE", 200))
    poll_timeout_s: float = field(default_factory=lambda: _env_float("SCORER_POLL_TIMEOUT_S", 1.0))
    fraud_threshold: float = field(default_factory=lambda: _env_float("FRAUD_THRESHOLD", 0.5))
    model_version: str = field(default_factory=lambda: _env("MODEL_VERSION", "xgb-v1"))
    # Optional static membership (KIP-345). Set a STABLE id per instance
    # (e.g. Kubernetes StatefulSet pod name) to avoid rebalances on restarts.
    group_instance_id: str = field(default_factory=lambda: _env("GROUP_INSTANCE_ID", ""))


@dataclass(frozen=True)
class ProducerSettings:
    tps: float = field(default_factory=lambda: _env_float("PRODUCER_TPS", 5.0))
    fraud_ratio: float = field(default_factory=lambda: _env_float("PRODUCER_FRAUD_RATIO", 0.10))
    # Size of the simulated customer pool. A small pool means the same
    # account appears many times -> shows per-key ordering on one partition.
    num_customers: int = field(default_factory=lambda: _env_int("PRODUCER_NUM_CUSTOMERS", 500))
    max_messages: int = field(default_factory=lambda: _env_int("PRODUCER_MAX_MESSAGES", 0))  # 0 = forever


@dataclass(frozen=True)
class AlertSettings:
    group_id: str = field(default_factory=lambda: _env("ALERT_GROUP_ID", "fraud-alert-notifier"))
    output_file: str = field(default_factory=lambda: _env("ALERT_OUTPUT_FILE", "/data/alerts.jsonl"))


LOG_LEVEL = _env("LOG_LEVEL", "INFO")
MODEL_DIR = _env(
    "MODEL_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "model")
)
