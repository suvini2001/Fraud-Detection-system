"""
Message contracts (schemas) for every Kafka topic.

Producer and consumers share these Pydantic models, so a message that does
not match the contract is rejected at the edge (producer/API) or routed to
the Dead Letter Queue (consumer) instead of crashing the pipeline.

`schema_version` lets us evolve the schema later without breaking old
consumers (they can branch on the version).
"""

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1

TxType = Literal["PAYMENT", "CASH_IN", "CASH_OUT", "DEBIT", "TRANSFER"]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TransactionEvent(BaseModel):
    """Raw transaction published to `transactions.raw` (PaySim-like fields)."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = SCHEMA_VERSION
    transaction_id: str = Field(min_length=1, max_length=64)
    event_time: datetime = Field(default_factory=utcnow)

    # PaySim "step" = hour since simulation start. Training kept day < 30,
    # so step must be in [0, 719]. hour_of_day / day_of_month are derived from it.
    step: int = Field(ge=0, le=719)
    type: TxType
    amount: float = Field(ge=0)

    nameOrig: str = Field(pattern=r"^C\d+$")          # sender: always a customer
    oldbalanceOrg: float = Field(ge=0)
    newbalanceOrig: float = Field(ge=0)

    nameDest: str = Field(pattern=r"^[CM]\d+$")       # receiver: customer or merchant
    oldbalanceDest: float = Field(ge=0)
    newbalanceDest: float = Field(ge=0)


class ScoredTransaction(BaseModel):
    """Model output published to `transactions.scored` (and `fraud.alerts` if fraud)."""

    schema_version: Literal[1] = SCHEMA_VERSION
    transaction_id: str
    nameOrig: str
    nameDest: str
    type: TxType
    amount: float
    fraud_probability: float = Field(ge=0, le=1)
    is_fraud: bool
    threshold: float
    model_version: str
    event_time: datetime
    scored_at: datetime = Field(default_factory=utcnow)
    # Where the input came from (useful for tracing/debugging)
    source_partition: Optional[int] = None
    source_offset: Optional[int] = None


class DeadLetter(BaseModel):
    """Envelope published to `transactions.dlq` when a message cannot be processed."""

    error_type: str
    error_message: str
    source_topic: str
    source_partition: int
    source_offset: int
    raw_value: Optional[str] = None      # original payload (as text) for replay/debugging
    failed_at: datetime = Field(default_factory=utcnow)
    consumer_group: str
