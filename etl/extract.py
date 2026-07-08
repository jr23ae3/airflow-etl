from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {
    "order_id",
    "order_timestamp",
    "customer_id",
    "product",
    "category",
    "units",
    "unit_price",
    "discount_pct",
    "shipping_cost",
    "state",
}


def extract_orders(source_csv: Path) -> pd.DataFrame:
    if not source_csv.exists():
        raise FileNotFoundError(f"Source file not found: {source_csv}")

    df = pd.read_csv(source_csv)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return df
