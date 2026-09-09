import json
import os
import joblib
import pandas as pd
import numpy as np
from kafka import KafkaConsumer


# ============================================================
# 1. LOAD MODEL AND PREPROCESSING ARTIFACTS
# ============================================================

print("Loading ML model and preprocessing artifacts...")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

model_path = os.path.join(
    BASE_DIR, "model", "fraud_xgb_model.pkl"
)

features_path = os.path.join(
    BASE_DIR, "model", "model_features.pkl"
)

frequency_path = os.path.join(
    BASE_DIR, "model", "frequency_maps.pkl"
)


# Load trained XGBoost model
model = joblib.load(model_path)

# Load the exact feature columns used during training
feature_columns = joblib.load(features_path)

# Load frequency encoding maps
frequency_maps = joblib.load(frequency_path)


print("ML model loaded successfully.")
print(f"Number of expected features: {len(feature_columns)}")


# ============================================================
# 2. CONNECT TO KAFKA
# ============================================================

consumer = KafkaConsumer(
    "transactions_topic",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda m: json.loads(
        m.decode("utf-8")
    ),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fraud-detection-consumer"
)


print("Connected to Kafka.")
print("Listening for incoming transactions...")


# ============================================================
# 3. PROCESS EACH TRANSACTION
# ============================================================

def process_transaction(data):

    print("\n----------------------------------------")
    print("New transaction received")
    print("----------------------------------------")

    # --------------------------------------------------------
    # A. FEATURE ENGINEERING
    # --------------------------------------------------------

    # Log transformation of transaction amount
    data["amount_log"] = np.log1p(data["amount"])

    # Difference between original and new sender balance
    data["balanceDiffOrig"] = (
        data["oldbalanceOrg"]
        - data["newbalanceOrig"]
    )

    # Difference between new and old destination balance
    data["balanceDiffDest"] = (
        data["newbalanceDest"]
        - data["oldbalanceDest"]
    )


    # --------------------------------------------------------
    # B. FREQUENCY ENCODING
    # --------------------------------------------------------

    # Get sender frequency map
    sender_counts = frequency_maps["sender_counts"]

    # Get receiver frequency map
    receiver_counts = frequency_maps["receiver_counts"]


    # Convert transaction sender ID into its training frequency
    data["sender_freqency"] = sender_counts.get(
        data["nameOrig"],
        0
    )

    # Convert transaction receiver ID into its training frequency
    data["receiver_freqency"] = receiver_counts.get(
        data["nameDest"],
        0
    )


    # --------------------------------------------------------
    # C. MERCHANT FEATURE
    # --------------------------------------------------------

    # Only create this feature if it was used during training
    if "is_merchant_dest" in feature_columns:

        data["is_merchant_dest"] = int(
            str(data["nameDest"]).startswith("M")
        )


    # --------------------------------------------------------
    # D. REMOVE RAW ID FEATURES
    # --------------------------------------------------------

    # These raw IDs were removed before model training
    data.pop("nameOrig", None)
    data.pop("nameDest", None)


    # --------------------------------------------------------
    # E. CONVERT RAW TRANSACTION INTO DATAFRAME
    # --------------------------------------------------------

    df = pd.DataFrame([data])


    # --------------------------------------------------------
    # F. ONE-HOT ENCODE TRANSACTION TYPE
    # --------------------------------------------------------

    df = pd.get_dummies(
        df,
        columns=["type"],
        drop_first=True
    )


    # --------------------------------------------------------
    # G. MATCH TRAINING FEATURE STRUCTURE
    # --------------------------------------------------------

    # Make sure the incoming transaction has exactly
    # the same columns and order as the training data
    df = df.reindex(
        columns=feature_columns,
        fill_value=0
    )


    # --------------------------------------------------------
    # H. MAKE FRAUD PREDICTION
    # --------------------------------------------------------

    prediction = model.predict(df)[0]


    # --------------------------------------------------------
    # I. DISPLAY RESULT
    # --------------------------------------------------------

    amount = data["amount"]

    if prediction == 1:

        print(
            f"🚨 FRAUD ALERT: "
            f"High-risk transaction detected! "
            f"Amount: ${amount:.2f}"
        )

    else:

        print(
            f"✅ SAFE: "
            f"Transaction appears legitimate. "
            f"Amount: ${amount:.2f}"
        )


    return prediction


# ============================================================
# 4. CONTINUOUS KAFKA STREAM PROCESSING
# ============================================================

def process_stream():

    print("\nFraud Detection Consumer Started.")
    print("Waiting for transactions...\n")

    for message in consumer:

        try:

            # Get transaction from Kafka message
            transaction = message.value

            # Process transaction through ML pipeline
            process_transaction(transaction)

        except Exception as e:

            print(
                f"❌ Error processing transaction: {e}"
            )


# ============================================================
# 5. START CONSUMER
# ============================================================

if __name__ == "__main__":

    process_stream()