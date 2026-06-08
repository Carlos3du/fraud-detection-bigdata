import math

import pandas as pd
from sklearn.preprocessing import StandardScaler

from schemas import (
    AMOUNT_CATEGORY_DUMMY_COLUMNS,
    EXPECTED_COLUMNS,
    NUMERIC_COLUMNS,
    OUTPUT_COLUMNS,
    V_COLUMNS,
)


def create_hour_bucket(time_in_seconds: float) -> str:
    hour = int(time_in_seconds // 3600) % 24
    return f"{hour:02d}:00-{hour:02d}:59"


def categorize_amount(amount: float) -> str:
    if amount < 10:
        return "baixo"
    if amount < 100:
        return "medio"
    if amount < 500:
        return "alto"
    return "muito_alto"


class TransactionTransformer:
    def __init__(self) -> None:
        self.amount_scaler = StandardScaler()

    def transform_record(self, record: dict) -> pd.DataFrame | None:
        df = pd.DataFrame([record])

        missing_columns = [
            column for column in EXPECTED_COLUMNS if column not in df.columns
        ]
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
        for column in V_COLUMNS:
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
        self.amount_scaler.partial_fit(amount_values)

        df["transaction_amount_scaled"] = self.amount_scaler.transform(amount_values)
        df["transaction_amount_log"] = df["Amount"].apply(math.log1p)
        df["transaction_hour_bucket"] = df["Time"].apply(create_hour_bucket)
        df["transaction_amount_category"] = df["Amount"].apply(categorize_amount)
        for column in AMOUNT_CATEGORY_DUMMY_COLUMNS:
            category = column.removeprefix("dummy_amount_cat_")
            df[column] = (df["transaction_amount_category"] == category).astype(int)
        df["is_fraud"] = df["Class"].map({1: "fraud", 0: "normal"})

        df = df.rename(
            columns={
                "Time": "transaction_time",
                "Amount": "transaction_amount",
                "Class": "class",
            }
        )

        return df[OUTPUT_COLUMNS]
