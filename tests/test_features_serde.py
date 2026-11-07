import os

import pandas as pd
import pytest

from app.common.config import MODEL_DIR
from app.common.features import build_feature_row
from app.common.schemas import TransactionEvent
from app.common.serde import DeserializationError, deserialize_value, serialize_value
from app.producer import TransactionGenerator

FEATURES = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest',
            'amount_log', 'balanceDiffOrig', 'balanceDiffDest', 'hour_of_day', 'day_of_month',
            'sender_freqency', 'receiver_freqency', 'is_merchant_dest',
            'type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER']
FREQ = {"sender_counts": {"C1": 3}, "receiver_counts": {"M2": 7}}


def ev(tx_type="TRANSFER", **kw):
    base = dict(transaction_id="t1", step=50, type=tx_type, amount=1000.0, nameOrig="C1",
                oldbalanceOrg=1000.0, newbalanceOrig=0.0, nameDest="M2",
                oldbalanceDest=0.0, newbalanceDest=0.0)
    base.update(kw)
    return TransactionEvent(**base)


@pytest.mark.parametrize("tx_type", ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"])
def test_type_one_hot_is_set_correctly(tx_type):
    row = build_feature_row(ev(tx_type), FEATURES, FREQ).iloc[0]
    onehot = {c: row[c] for c in FEATURES if c.startswith("type_")}
    expected = {c: int(c == f"type_{tx_type}") for c in onehot}
    assert onehot == expected   # CASH_IN -> all zeros (dropped baseline)


def test_old_code_lost_the_type_feature():
    """Documents the original bug: get_dummies(drop_first=True) on one row."""
    df = pd.get_dummies(pd.DataFrame([{"type": "TRANSFER"}]), columns=["type"], drop_first=True)
    df = df.reindex(columns=FEATURES, fill_value=0)
    assert df["type_TRANSFER"].iloc[0] == 0      # should have been 1


def test_derived_features():
    row = build_feature_row(ev(step=50), FEATURES, FREQ).iloc[0]
    assert (row.hour_of_day, row.day_of_month) == (2, 2)
    assert (row.sender_freqency, row.receiver_freqency, row.is_merchant_dest) == (3, 7, 1)
    assert list(build_feature_row(ev(), FEATURES, FREQ).columns) == FEATURES


def test_serde_roundtrip_and_validation():
    e = ev()
    assert deserialize_value(serialize_value(e), TransactionEvent) == e
    for bad in [b"\xff\xfe", b"{oops", b'{"transaction_id": "x"}', None]:
        with pytest.raises(DeserializationError):
            deserialize_value(bad, TransactionEvent)
    # unknown fields are rejected (extra="forbid")
    with pytest.raises(Exception):
        TransactionEvent(**{**ev().model_dump(), "hacker": 1})


def test_generator_reuses_customer_keys():
    g = TransactionGenerator(num_customers=10, fraud_ratio=0.2, seed=1)
    events = [g.next() for _ in range(500)]
    assert len({e.nameOrig for e in events}) <= 10           # same keys repeat
    assert all(e.type in ("TRANSFER", "CASH_OUT") for e in events if e.amount >= 50_000)


@pytest.mark.skipif(not os.path.exists(os.path.join(MODEL_DIR, "fraud_xgb_model.pkl")),
                    reason="model artifacts not present")
def test_real_model_scores_fraud_pattern_higher():
    from app.common.model import FraudScorer
    scorer = FraudScorer(MODEL_DIR)
    fraud = ev("TRANSFER", amount=300_000, oldbalanceOrg=300_000, newbalanceOrig=0,
               nameDest="C123456789")
    normal = ev("PAYMENT", amount=120, oldbalanceOrg=20_000, newbalanceOrig=19_880,
                nameDest="M123456789")
    p_fraud, _ = scorer.score(fraud)
    p_normal, _ = scorer.score(normal)
    print(f"p_fraud={p_fraud:.4f} p_normal={p_normal:.4f}")
    assert p_fraud > p_normal
