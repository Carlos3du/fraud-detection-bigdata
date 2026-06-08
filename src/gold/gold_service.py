import time
import traceback
from io import BytesIO

import boto3
import joblib
import pandas as pd
from decouple import config as env

MINIO_ENDPOINT = env("MINIO_ENDPOINT", default="http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", default="minioadmin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", default="minioadmin")
SILVER_BUCKET = env("SILVER_BUCKET", default="silver")
GOLD_BUCKET = env("GOLD_BUCKET", default="gold")
MODEL_PATH = env("MODEL_PATH", default="/app/models/xgb_model.pkl")
POLL_INTERVAL = int(env("POLL_INTERVAL", default="10"))

DEFAULT_FEATURE_COLUMNS = (
    [
        "transaction_time",
    ]
    + [f"V{i}" for i in range(1, 29)]
    + [
        "transaction_amount",
        "transaction_amount_scaled",
        "transaction_amount_log",
        "dummy_amount_cat_alto",
        "dummy_amount_cat_baixo",
        "dummy_amount_cat_medio",
        "dummy_amount_cat_muito_alto",
    ]
)
AMOUNT_CATEGORY_DUMMY_COLUMNS = [
    "dummy_amount_cat_baixo",
    "dummy_amount_cat_medio",
    "dummy_amount_cat_alto",
    "dummy_amount_cat_muito_alto",
]

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


def ensure_bucket(bucket: str) -> None:
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket criado: {bucket}")


def list_silver_keys() -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix="transformed/"):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def list_gold_keys() -> set[str]:
    keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix="predictions/"):
        keys.update(obj["Key"] for obj in page.get("Contents", []))
    return keys


def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def get_model_and_features(artifact) -> tuple[object, list[str]]:
    if isinstance(artifact, dict):
        model = artifact["model"]
    else:
        model = artifact

    feature_names = getattr(model, "feature_names_in_", None)
    if feature_names is None:
        feature_names = DEFAULT_FEATURE_COLUMNS

    return model, list(feature_names)


def build_features(df: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    if "transaction_amount_category" in df.columns:
        for column in AMOUNT_CATEGORY_DUMMY_COLUMNS:
            if column not in df.columns:
                category = column.removeprefix("dummy_amount_cat_")
                df[column] = (df["transaction_amount_category"] == category).astype(int)

    missing_columns = [column for column in feature_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing feature columns in silver data: {missing_columns}")

    return df[feature_columns]


def classify_risk(prob: float) -> str:
    if prob >= 0.7:
        return "alto"
    if prob >= 0.3:
        return "medio"
    return "baixo"


def process_file(key: str, model, feature_columns: list[str]) -> None:
    silver_df = read_parquet(SILVER_BUCKET, key)
    features = build_features(silver_df, feature_columns)

    probs = model.predict_proba(features)[:, 1]
    preds = (probs >= 0.5).astype(int)

    gold_df = silver_df.copy()
    gold_df["fraud_probability"] = probs.round(4)
    gold_df["is_fraud_pred"] = preds
    gold_df["risk_level"] = [classify_risk(p) for p in probs]

    buffer = BytesIO()
    gold_df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    gold_key = key.replace("transformed/", "predictions/")
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key=gold_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    fraud_count = int(preds.sum())
    print(f"[gold] {key} → {gold_key} | {len(silver_df)} registros | {fraud_count} fraudes detectadas")


def main() -> None:
    print("Carregando modelo...")
    artifact = joblib.load(MODEL_PATH)
    model, feature_columns = get_model_and_features(artifact)
    print(f"Modelo carregado com {len(feature_columns)} features.")

    ensure_bucket(SILVER_BUCKET)
    ensure_bucket(GOLD_BUCKET)

    while True:
        try:
            silver_keys = list_silver_keys()
            gold_keys = list_gold_keys()

            pending = [
                k for k in silver_keys
                if k.replace("transformed/", "predictions/") not in gold_keys
            ]

            if pending:
                print(f"[gold] {len(pending)} arquivo(s) novo(s) para processar")
                for key in pending:
                    process_file(key, model, feature_columns)
            else:
                print(f"[gold] Nenhum arquivo novo. Aguardando {POLL_INTERVAL}s...")

        except Exception:
            traceback.print_exc()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
