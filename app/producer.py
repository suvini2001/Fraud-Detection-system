"  Generate a transaction → convert it to JSON → send it to Kafka."

import json
import time
import random
from kafka import KafkaProducer


# Connect to Kafka

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # The Kafka server is available at the Docker service called kafka, port 9092.
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # For every Python object I send to Kafka, convert it to JSON and then to UTF-8 bytes.
)



"""
______________________________________________
| PRODUCER           |              CONSUMER    |
| -------------------|------------------------  |
| Python dict        |             bytes        |
|    ↓               |                ↓         |
| json.dumps()       |            decode()      |
|    ↓               |                ↓         |
| UTF-8 bytes        |            JSON string   |
|    ↓               |                ↓         |
|   KAFKA            |            json.loads()  |
|                    |               ↓          |
|                    |         Python dictionary|
|------------------------------------------------


"""
# Kafka topic
topic_name = "transactions_topic"


def generate_mock_transaction():
    """
    Generate a raw transaction similar to the PaySim dataset.

    The producer sends only raw transaction fields.
    Feature engineering is handled by the Kafka consumer.
    """

    # 10% chance of generating a suspicious transaction
    simulate_fraud = random.random() < 0.10

    if simulate_fraud:
        # Fraud-like transaction
        tx_type = random.choice(["TRANSFER", "CASH_OUT"])

        amount = round(random.uniform(50000.0, 500000.0), 2)

        # Simulate an account being completely drained
        oldbalanceOrg = amount
        newbalanceOrig = 0.0

        oldbalanceDest = 0.0
        newbalanceDest = amount

        # Customer-to-customer transaction
        nameDest = f"C{random.randint(100000000, 999999999)}"

    else:
        # Normal transaction
        tx_type = random.choice([
            "PAYMENT",
            "CASH_IN",
            "CASH_OUT",
            "DEBIT",
            "TRANSFER"
        ])

        amount = round(random.uniform(10.0, 5000.0), 2)

        oldbalanceOrg = round(
            random.uniform(5000.0, 50000.0), 2
        )

        newbalanceOrig = max(
            0.0,
            oldbalanceOrg - amount
        )

        oldbalanceDest = round(
            random.uniform(100.0, 20000.0), 2
        )

        newbalanceDest = oldbalanceDest + amount

        # PAYMENT → Merchant
        # Other transaction types → Customer
        if tx_type == "PAYMENT":
            nameDest = f"M{random.randint(100000000, 999999999)}"
        else:
            nameDest = f"C{random.randint(100000000, 999999999)}"
 # Generate raw transaction
    transaction = {
        "amount": amount,
        "oldbalanceOrg": oldbalanceOrg,
        "newbalanceOrig": newbalanceOrig,
        "oldbalanceDest": oldbalanceDest,
        "newbalanceDest": newbalanceDest,
        "hour_of_day": random.randint(0, 23),
        "day_of_month": random.randint(0, 29),
        "nameOrig": f"C{random.randint(100000000, 999999999)}",
        "nameDest": nameDest,
        "type": tx_type
    }

    return transaction


def simulate_data_stream():
    """
    Continuously generate transactions
    and send them to Kafka.
    """

    print("Starting transaction data stream...")

    while True:

        transaction = generate_mock_transaction()

        # Send transaction to Kafka
        producer.send(
            topic_name,
            transaction
        )

        # Make sure the message is sent
        producer.flush()

        print(
            f"Sent [{transaction['type']}] "
            f"Amount: ${transaction['amount']} "
            f"→ {transaction['nameDest']}"
        )

        # Wait before generating the next transaction
        time.sleep(2)


if __name__ == "__main__":
    simulate_data_stream()
