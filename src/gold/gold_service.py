import time
import traceback
from io import BytesIO

import boto3
import joblib
import numpy as np
import pandas as pd
from decouple import config as env

MINIO_ENDPOINT = env("MINIO_ENDPOINT", default="http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", default="minioadmin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", default="minioadmin")
SILVER_BUCKET = env("SILVER_BUCKET", default="silver")
GOLD_BUCKET = env("GOLD_BUCKET", default="gold")
MODEL_PATH = env("MODEL_PATH", default="/app/dados/model_fraud.pkl")
POLL_INTERVAL = int(env("POLL_INTERVAL", default="10"))

FEATURE_COLUMNS = (
    ["Time"]
    + [f"V{i}" for i in range(1, 29)]
    + ["Amount", "Hour", "log_amount", "hour", "diff_from_mean", "v4_v14_inter"]
)

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
    response = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix="transformed/")
    return [obj["Key"] for obj in response.get("Contents", [])]


def list_gold_keys() -> set[str]:
    response = s3.list_objects_v2(Bucket=GOLD_BUCKET, Prefix="predictions/")
    return {obj["Key"] for obj in response.get("Contents", [])}


def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def build_features(df: pd.DataFrame, mean_amount: float) -> pd.DataFrame:
    feat = pd.DataFrame()
    feat["Time"] = df["transaction_time"]
    for v in [f"V{i}" for i in range(1, 29)]:
        feat[v] = df[v]
    feat["Amount"] = df["transaction_amount"]
    feat["Hour"] = np.floor(feat["Time"] / 3600)
    feat["log_amount"] = np.log1p(feat["Amount"])
    feat["hour"] = (feat["Time"] // 3600) % 24
    feat["diff_from_mean"] = feat["Amount"] - mean_amount
    feat["v4_v14_inter"] = feat["V4"] * feat["V14"]
    return feat[FEATURE_COLUMNS]


def classify_risk(prob: float) -> str:
    if prob >= 0.7:
        return "alto"
    if prob >= 0.3:
        return "medio"
    return "baixo"


def process_file(key: str, model, mean_amount: float) -> None:
    silver_df = read_parquet(SILVER_BUCKET, key)
    features = build_features(silver_df, mean_amount)

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
    model = artifact["model"]
    mean_amount = artifact["mean_amount"]
    print(f"Modelo carregado. mean_amount={mean_amount:.4f}")

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
                    process_file(key, model, mean_amount)
            else:
                print(f"[gold] Nenhum arquivo novo. Aguardando {POLL_INTERVAL}s...")

        except Exception:
            traceback.print_exc()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
