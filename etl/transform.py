import pandas as pd


def transform_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    clean = df.copy()

    clean["order_timestamp"] = pd.to_datetime(clean["order_timestamp"], utc=True)
    clean["units"] = pd.to_numeric(clean["units"], downcast="integer")
    clean["unit_price"] = pd.to_numeric(clean["unit_price"])
    clean["discount_pct"] = pd.to_numeric(clean["discount_pct"])
    clean["shipping_cost"] = pd.to_numeric(clean["shipping_cost"])

    clean = clean[clean["units"] > 0]
    clean = clean[clean["unit_price"] >= 0]
    clean = clean.drop_duplicates(subset=["order_id"], keep="last")

    clean["gross_revenue"] = clean["units"] * clean["unit_price"]
    clean["discount_amount"] = clean["gross_revenue"] * (clean["discount_pct"] / 100.0)
    clean["net_revenue"] = clean["gross_revenue"] - clean["discount_amount"]
    clean["order_date"] = clean["order_timestamp"].dt.date

    daily_sales = (
        clean.groupby("order_date", as_index=False)
        .agg(
            order_count=("order_id", "nunique"),
            gross_revenue=("gross_revenue", "sum"),
            discount_amount=("discount_amount", "sum"),
            net_revenue=("net_revenue", "sum"),
            shipping_total=("shipping_cost", "sum"),
        )
        .sort_values("order_date")
    )
    daily_sales["avg_order_value"] = daily_sales["net_revenue"] / daily_sales["order_count"]

    category_sales = (
        clean.groupby("category", as_index=False)
        .agg(
            order_count=("order_id", "nunique"),
            units_sold=("units", "sum"),
            net_revenue=("net_revenue", "sum"),
        )
        .sort_values("net_revenue", ascending=False)
    )

    return clean, daily_sales, category_sales
