EXPECTED_COLUMNS = ["Time", "Amount", "Class"] + [f"V{i}" for i in range(1, 29)]
NUMERIC_COLUMNS = ["Time", "Amount"] + [f"V{i}" for i in range(1, 29)]
V_COLUMNS = [f"V{i}" for i in range(1, 29)]

AMOUNT_CATEGORY_DUMMY_COLUMNS = [
    "dummy_amount_cat_baixo",
    "dummy_amount_cat_medio",
    "dummy_amount_cat_alto",
    "dummy_amount_cat_muito_alto",
]

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
    + AMOUNT_CATEGORY_DUMMY_COLUMNS
    + V_COLUMNS
    + ["class"]
)
