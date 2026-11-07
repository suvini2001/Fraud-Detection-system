"""Loads the trained XGBoost model + preprocessing artifacts once and scores events."""

import logging
import os
import time
from typing import Tuple

import joblib

from .features import build_feature_row
from .schemas import TransactionEvent

log = logging.getLogger(__name__)


class FraudScorer:
    def __init__(self, model_dir: str, threshold: float = 0.5):
        t0 = time.time()
        self.model = joblib.load(os.path.join(model_dir, "fraud_xgb_model.pkl"))
        self.feature_columns = list(joblib.load(os.path.join(model_dir, "model_features.pkl")))
        self.frequency_maps = joblib.load(os.path.join(model_dir, "frequency_maps.pkl"))
        self.threshold = threshold
        log.info(
            "Model loaded in %.1fs (%d features, threshold=%.2f)",
            time.time() - t0, len(self.feature_columns), threshold,
        )

    def score(self, event: TransactionEvent) -> Tuple[float, bool]:
        X = build_feature_row(event, self.feature_columns, self.frequency_maps)
        prob = float(self.model.predict_proba(X)[0][1])
        return prob, prob >= self.threshold
