"""
Feature engineering shared by the API (/predict) and the Kafka scorer.

Keeping ONE implementation avoids "training/serving skew" (the API and the
stream computing features differently).

BUG FIX vs the original code:
  The old code called  pd.get_dummies(df, columns=["type"], drop_first=True)
  on a ONE-row DataFrame. One row has only one category, and drop_first=True
  drops it, so every type_* column became 0 after reindex() - the model
  never saw the transaction type. Here the one-hot columns are built
  directly from the training feature list instead.
"""

from typing import Dict, List

import numpy as np
import pandas as pd

from .schemas import TransactionEvent


def build_feature_row(
    event: TransactionEvent,
    feature_columns: List[str],
    frequency_maps: Dict[str, Dict[str, int]],
) -> pd.DataFrame:
    row = {
        "amount": event.amount,
        "oldbalanceOrg": event.oldbalanceOrg,
        "newbalanceOrig": event.newbalanceOrig,
        "oldbalanceDest": event.oldbalanceDest,
        "newbalanceDest": event.newbalanceDest,
        "amount_log": float(np.log1p(event.amount)),
        "balanceDiffOrig": event.oldbalanceOrg - event.newbalanceOrig,
        "balanceDiffDest": event.newbalanceDest - event.oldbalanceDest,
        # Same derivation as the training notebook
        "hour_of_day": event.step % 24,
        "day_of_month": event.step // 24,
        # Frequency encoding learned ONLY from the training split (unseen -> 0)
        "sender_freqency": frequency_maps["sender_counts"].get(event.nameOrig, 0),
        "receiver_freqency": frequency_maps["receiver_counts"].get(event.nameDest, 0),
        "is_merchant_dest": int(event.nameDest.startswith("M")),
    }

    # One-hot for transaction type, matching training columns
    # (training used drop_first=True, so CASH_IN is the all-zeros baseline).
    for col in feature_columns:
        if col.startswith("type_"):
            row[col] = int(event.type == col[len("type_"):])

    # Exact same columns, same order as training; anything missing -> 0
    return pd.DataFrame([row]).reindex(columns=feature_columns, fill_value=0)
