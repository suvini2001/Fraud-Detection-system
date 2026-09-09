from fastapi import FastAPI
import joblib
from pydantic import BaseModel
import pandas as pd
import numpy as np
from pathlib import Path


# - initilaize the app and load artifacts to memory
app=FastAPI( title="Fraud Detection API")

# load the models generated from  my colab notebook

# Resolve the path problem 

BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR / "model"

model=joblib.load(MODEL_DIR/"fraud_xgb_model.pkl")
feature_columns=joblib.load(MODEL_DIR/"model_features.pkl")
frequency_maps=joblib.load(MODEL_DIR/"frequency_maps.pkl")



#Step 3: Create the Data Validation Schema (BaseModel)

"""
This is where Pydantic comes in.
creating a validation schema using Pydantic. This acts as a strict bouncer for your API. 
It ensures that any incoming request contains exactly the right fields with the correct data types 
before the model tries to process it. If a request is missing an amount or sends text instead of a number,
the API automatically rejects it with an error.

"""

class Transaction(BaseModel):
    step: int
    type: str
    amount: float
    oldbalanceOrg: float
    newbalanceOrig: float
    oldbalanceDest: float
    newbalanceDest: float
    nameOrig: str
    nameDest: str


#Step 4: Define the Prediction Route

"""
create an endpoint (e.g., /predict) that accepts HTTP POST requests containing the Transaction data.

Inside this function, you must write the logic that takes the raw incoming data, applies the exact same feature engineering you did during 
training (calculating balance differences, mapping frequency dictionaries, flagging merchants), and then runs model.predict().

"""


@app.post("/predict")
def predict_fraud(transaction: Transaction):

    # Convert Pydantic object to dictionary
    data = transaction.model_dump()

    # --------------------------------------------------
    # 1. RECREATE FEATURES CREATED DURING TRAINING
    # --------------------------------------------------

    # Log transformation of transaction amount
    data["amount_log"] = np.log1p(data["amount"])

    # Origin account balance difference
    data["balanceDiffOrig"] = (
        data["oldbalanceOrg"] - data["newbalanceOrig"]
    )

    # Destination account balance difference
    data["balanceDiffDest"] = (
        data["newbalanceDest"] - data["oldbalanceDest"]
    )

    # Extract hour from step
    data["hour_of_day"] = data["step"] % 24

    # Extract day from step
    data["day_of_month"] = data["step"] // 24


    # --------------------------------------------------
    # 2. APPLY TRAINING-ONLY FREQUENCY MAPS
    # --------------------------------------------------

    data["sender_freqency"] = (
        frequency_maps["sender_counts"]
        .get(data["nameOrig"], 0)
    )

    data["receiver_freqency"] = (
        frequency_maps["receiver_counts"]
        .get(data["nameDest"], 0)
    )


    # --------------------------------------------------
    # 3. MERCHANT DESTINATION FEATURE
    # --------------------------------------------------

    data["is_merchant_dest"] = (
        1 if str(data["nameDest"]).startswith("M") else 0
    )


    # --------------------------------------------------
    # 4. REMOVE RAW CATEGORICAL ACCOUNT IDs
    # --------------------------------------------------

    del data["nameOrig"]
    del data["nameDest"]

    # step was used to create time features
    # and was dropped during training
    del data["step"]


    # --------------------------------------------------
    # 5. CONVERT TO DATAFRAME
    # --------------------------------------------------

    df = pd.DataFrame([data])


    # --------------------------------------------------
    # 6. ONE-HOT ENCODE TRANSACTION TYPE
    # --------------------------------------------------

    df = pd.get_dummies(
        df,
        columns=["type"],
        drop_first=True
    )


    # --------------------------------------------------
    # 7. MATCH EXACT TRAINING FEATURES
    # --------------------------------------------------

    df = df.reindex(
        columns=feature_columns,
        fill_value=0
    )


    # --------------------------------------------------
    # 8. MAKE PREDICTION
    # --------------------------------------------------

    prediction = model.predict(df)[0]


    # --------------------------------------------------
    # 9. RETURN JSON RESPONSE
    # --------------------------------------------------

    return {
        "is_fraud": int(prediction)
    }