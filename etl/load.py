from sqlalchemy import create_engine, text

import pandas as pd


def load_to_postgres(
    orders: pd.DataFrame,
    daily_sales: pd.DataFrame,
    category_sales: pd.DataFrame,
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_password: str,
) -> None:
    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    with engine.begin() as conn:
        conn.execute(text("truncate table raw.orders"))
        conn.execute(text("truncate table analytics.daily_sales"))
        conn.execute(text("truncate table analytics.category_sales"))

    orders.to_sql("orders", con=engine, schema="raw", if_exists="append", index=False)
    daily_sales.to_sql(
        "daily_sales", con=engine, schema="analytics", if_exists="append", index=False
    )
    category_sales.to_sql(
        "category_sales", con=engine, schema="analytics", if_exists="append", index=False
    )
