import os
from pathlib import Path

from extract import extract_orders
from load import load_to_postgres
from transform import transform_orders


SOURCE_CSV = Path(os.getenv("SOURCE_CSV", "/workspace/data/source/orders_raw.csv"))
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "warehouse")
DB_USER = os.getenv("POSTGRES_USER", "etl")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl")


def main() -> None:
    extracted_df = extract_orders(SOURCE_CSV)
    orders_df, daily_sales_df, category_sales_df = transform_orders(extracted_df)

    load_to_postgres(
        orders=orders_df,
        daily_sales=daily_sales_df,
        category_sales=category_sales_df,
        db_host=DB_HOST,
        db_port=DB_PORT,
        db_name=DB_NAME,
        db_user=DB_USER,
        db_password=DB_PASSWORD,
    )

    print(
        "ETL completed: "
        f"orders={len(orders_df)}, daily_sales={len(daily_sales_df)}, "
        f"category_sales={len(category_sales_df)}"
    )


if __name__ == "__main__":
    main()
