import json
import math
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
from decouple import config as env
from kafka import KafkaConsumer
from sklearn.preprocessing import StandardScaler

KAFKA_BROKER = env("KAFKA_BROKER", default="localhost:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", default="creditcard")
MINIO_ENDPOINT = env("MINIO_ENDPOINT", default="http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", default="minioadmin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", default="minioadmin")
MINIO_BUCKET = env("MINIO_BUCKET", default="silver")
LOCAL_OUTPUT_DIR = Path("dados/transformed")

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
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

if MINIO_BUCKET not in [bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])]:
    s3_client.create_bucket(Bucket=MINIO_BUCKET)

LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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


def upload_parquet_to_minio(df: pd.DataFrame, partition: int, offset: int) -> str:
    buffer = BytesIO()
    object_key = f"transformed/partition={partition}/offset={offset}.parquet"
    local_file = LOCAL_OUTPUT_DIR / f"partition={partition}_offset={offset}.parquet"

    df.to_parquet(local_file, engine="pyarrow", index=False)
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    return object_key


for message in consumer:
    transformed_df = transform_record(message.value)
    if transformed_df is None:
        continue

    object_key = upload_parquet_to_minio(
        transformed_df,
        partition=message.partition,
        offset=message.offset,
    )

    print(
        json.dumps(
            {
                "input_topic": KAFKA_TOPIC,
                "minio_bucket": MINIO_BUCKET,
                "minio_object": object_key,
                "record": transformed_df.iloc[0].to_dict(),
            },
            ensure_ascii=False,
        )
    )
