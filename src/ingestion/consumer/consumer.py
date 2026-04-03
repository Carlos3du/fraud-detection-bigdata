import json
import math

import pandas as pd
from decouple import config as env
from kafka import KafkaConsumer, KafkaProducer
from sklearn.preprocessing import StandardScaler

KAFKA_BROKER = env("KAFKA_BROKER", default="localhost:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", default="creditcard")
KAFKA_OUTPUT_TOPIC = env("KAFKA_OUTPUT_TOPIC", default="creditcard-transformed")

EXPECTED_COLUMNS = ["Time", "Amount", "Class"] + [f"V{i}" for i in range(1, 29)]
NUMERIC_COLUMNS = ["Time", "Amount"] + [f"V{i}" for i in range(1, 29)]
OUTPUT_COLUMNS = (
    [
        "is_fraud",
        "transaction_time",
        "transaction_hour_bucket",
        "transaction_amount",
        "transaction_amount_scaled",
        "transaction_amount_log",
        "transaction_amount_category",
    ]
    + [f"V{i}" for i in range(1, 29)]
    + ["class"]
)

amount_scaler = StandardScaler()
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def categorize_amount(amount: float) -> str:
    if amount < 10:
        return "baixo"
    if amount < 100:
        return "medio"
    if amount < 500:
        return "alto"
    return "muito_alto"


def create_hour_bucket(time_in_seconds: float) -> str:
    hour = int(time_in_seconds // 3600) % 24
    return f"{hour:02d}:00-{hour:02d}:59"


def transform_record(record: dict) -> pd.DataFrame | None:
    df = pd.DataFrame([record])

    missing_columns = [column for column in EXPECTED_COLUMNS if column not in df.columns]
    if missing_columns:
        print(f"Skipping record with missing columns: {missing_columns}")
        return None

    df = df[EXPECTED_COLUMNS].dropna()
    if df.empty:
        print("Skipping record with null values.")
        return None

    for column in NUMERIC_COLUMNS + ["Class"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna()
    if df.empty:
        print("Skipping record with invalid numeric values.")
        return None

    df["Time"] = df["Time"].astype(float)
    df["Amount"] = df["Amount"].astype(float)
    for column in [f"V{i}" for i in range(1, 29)]:
        df[column] = df[column].astype(float)
    df["Class"] = df["Class"].astype(int)

    has_invalid_range = (
        (df["Time"] < 0).any()
        or (df["Amount"] < 0).any()
        or (~df["Class"].isin([0, 1])).any()
    )
    if has_invalid_range:
        print("Skipping record with values outside valid ranges.")
        return None

    amount_values = df[["Amount"]]
    amount_scaler.partial_fit(amount_values)

    df["transaction_amount_scaled"] = amount_scaler.transform(amount_values)
    df["transaction_amount_log"] = df["Amount"].apply(math.log1p)
    df["transaction_hour_bucket"] = df["Time"].apply(create_hour_bucket)
    df["transaction_amount_category"] = df["Amount"].apply(categorize_amount)
    df["is_fraud"] = df["Class"].map({1: "fraud", 0: "normal"})

    df = df.rename(
        columns={
            "Time": "transaction_time",
            "Amount": "transaction_amount",
            "Class": "class",
        }
    )

    return df[OUTPUT_COLUMNS]


for message in consumer:
    transformed_df = transform_record(message.value)
    if transformed_df is None:
        continue

    transformed_record = transformed_df.iloc[0].to_dict()
    producer.send(KAFKA_OUTPUT_TOPIC, transformed_record)
    producer.flush()

    print(
        json.dumps(
            {
                "input_topic": KAFKA_TOPIC,
                "output_topic": KAFKA_OUTPUT_TOPIC,
                "record": transformed_record,
            },
            ensure_ascii=False,
        )
    )
