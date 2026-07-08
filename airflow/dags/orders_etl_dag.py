from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text


PIPELINE_ROOT = Path("/opt/pipeline")
if str(PIPELINE_ROOT / "etl") not in sys.path:
    sys.path.append(str(PIPELINE_ROOT / "etl"))

from extract import extract_orders
from load import load_to_postgres
from transform import transform_orders


LOGGER = logging.getLogger("orders_etl_dag")

STAGING_DIR = Path("/opt/pipeline/data/staging")
EXTRACTED_FILE = STAGING_DIR / "orders_extracted.csv"
ORDERS_FILE = STAGING_DIR / "orders_transformed.csv"
DAILY_FILE = STAGING_DIR / "daily_sales_transformed.csv"
CATEGORY_FILE = STAGING_DIR / "category_sales_transformed.csv"


def _db_settings() -> dict[str, str | int]:
    return {
        "db_host": Variable.get("etl_postgres_host", default_var="postgres"),
        "db_port": int(Variable.get("etl_postgres_port", default_var="5432")),
        "db_name": Variable.get("etl_postgres_db", default_var="warehouse"),
        "db_user": Variable.get("etl_postgres_user", default_var="etl"),
        "db_password": Variable.get("etl_postgres_password", default_var="etl"),
    }


def extract_task() -> None:
    source_csv = Path(
        Variable.get(
            "etl_source_csv",
            default_var="/opt/pipeline/data/source/orders_raw.csv",
        )
    )
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df = extract_orders(source_csv)
    df.to_csv(EXTRACTED_FILE, index=False)
    LOGGER.info("Extracted %s rows from %s", len(df), source_csv)


def transform_task() -> None:
    if not EXTRACTED_FILE.exists():
        raise FileNotFoundError(f"Missing extracted dataset: {EXTRACTED_FILE}")

    df = pd.read_csv(EXTRACTED_FILE)
    orders_df, daily_sales_df, category_sales_df = transform_orders(df)

    orders_df.to_csv(ORDERS_FILE, index=False)
    daily_sales_df.to_csv(DAILY_FILE, index=False)
    category_sales_df.to_csv(CATEGORY_FILE, index=False)

    LOGGER.info(
        "Transformed datasets: orders=%s, daily_sales=%s, category_sales=%s",
        len(orders_df),
        len(daily_sales_df),
        len(category_sales_df),
    )


def load_task() -> None:
    for p in [ORDERS_FILE, DAILY_FILE, CATEGORY_FILE]:
        if not p.exists():
            raise FileNotFoundError(f"Missing transformed dataset: {p}")

    orders_df = pd.read_csv(ORDERS_FILE)
    daily_sales_df = pd.read_csv(DAILY_FILE)
    category_sales_df = pd.read_csv(CATEGORY_FILE)

    load_to_postgres(
        orders=orders_df,
        daily_sales=daily_sales_df,
        category_sales=category_sales_df,
        **_db_settings(),
    )

    LOGGER.info("Loaded transformed data into PostgreSQL")


def quality_task() -> None:
    settings = _db_settings()
    engine = create_engine(
        "postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}".format(
            **settings
        )
    )
    sql = """
    select
      (select count(*) from raw.orders) as orders,
      (select count(*) from analytics.daily_sales) as daily_sales,
      (select count(*) from analytics.category_sales) as category_sales
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql)).mappings().one()

    LOGGER.info("Quality check counts => %s", dict(row))
    if row["orders"] == 0:
        raise ValueError("Quality check failed: raw.orders is empty")


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="orders_etl_pipeline",
    description="Airflow ETL pipeline using Python + pandas + PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["etl", "pandas", "postgres"],
) as dag:
    start = BashOperator(
        task_id="start_log",
        bash_command='echo "Starting Orders ETL DAG run: {{ ds }}"',
    )

    extract = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load_orders",
        python_callable=load_task,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_task,
    )

    finish = BashOperator(
        task_id="finish_log",
        bash_command='echo "Orders ETL DAG completed successfully"',
    )

    start >> extract >> transform >> load >> quality_check >> finish
