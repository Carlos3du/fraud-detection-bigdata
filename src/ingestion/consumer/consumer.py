import atexit
import json
import math
import traceback
from io import BytesIO

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
current_window: int | None = None
current_window_rows: list[pd.DataFrame] = []
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

try:
    print("Checking existing MinIO buckets...")
    buckets = s3_client.list_buckets().get("Buckets", [])
    bucket_names = [bucket["Name"] for bucket in buckets]
    print(f"Buckets found: {bucket_names}")
    if MINIO_BUCKET not in bucket_names:
        print(f"Creating bucket: {MINIO_BUCKET}")
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket created: {MINIO_BUCKET}")
    else:
        print(f"Bucket already exists: {MINIO_BUCKET}")
except Exception as exc:
    print(f"Failed to initialize MinIO bucket '{MINIO_BUCKET}': {exc}")
    traceback.print_exc()
    raise


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


def upload_parquet_to_minio(df: pd.DataFrame, window: int) -> str:
    buffer = BytesIO()
    object_key = f"transformed/window_24s={window}.parquet"

    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    print(f"Uploading parquet to MinIO: bucket={MINIO_BUCKET}, key={object_key}")
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    print(f"Upload completed: bucket={MINIO_BUCKET}, key={object_key}")
    return object_key


def flush_current_window() -> str | None:
    global current_window_rows

    if current_window is None or not current_window_rows:
        return None

    window_df = pd.concat(current_window_rows, ignore_index=True)
    object_key = upload_parquet_to_minio(window_df, current_window)
    current_window_rows = []
    return object_key


@atexit.register
def flush_on_shutdown() -> None:
    object_key = flush_current_window()
    if object_key is not None:
        print(f"Final 24-second parquet flushed on shutdown: {object_key}")


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
    print(f"Received Kafka message: partition={message.partition}, offset={message.offset}")

    transformed_df = transform_record(message.value)
    if transformed_df is None:
        print(
            f"Skipping Kafka message after validation: partition={message.partition}, offset={message.offset}"
        )
        continue

    record_window = int(transformed_df.iloc[0]["transaction_time"] // 24)

    if current_window is None:
        current_window = record_window

    if record_window != current_window:
        object_key = flush_current_window()
        if object_key is not None:
            print(f"24-second parquet flushed: {object_key}")
        current_window = record_window

    current_window_rows.append(transformed_df)

    print(
        json.dumps(
            {
                "input_topic": KAFKA_TOPIC,
                "minio_bucket": MINIO_BUCKET,
                "current_window": current_window,
                "buffered_rows": len(current_window_rows),
                "record": transformed_df.iloc[0].to_dict(),
            },
            ensure_ascii=False,
        )
    )
