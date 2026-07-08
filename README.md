# Airflow ETL Pipeline

This repository moves the Python ETL pipeline into Airflow orchestration with:
- DAGs
- Scheduling
- Operators
- Variables
- Logging
- Retries

Core ETL stack:
- Python
- pandas
- PostgreSQL

## Airflow Features Implemented
- DAG: `orders_etl_pipeline`
- Schedule: `0 2 * * *` (daily at 02:00)
- Operators: `PythonOperator`, `BashOperator`
- Variables: source file path and Postgres connection settings
- Logging: task-level structured logs via Python logger
- Retries: 2 retries with 2-minute delay

## Pipeline Flow
1. Extract raw orders from CSV
2. Transform with pandas (cleaning + aggregations)
3. Load into Postgres raw/analytics tables
4. Run quality checks and log row counts

## Project Structure
```text
.
├── airflow/
│   ├── Dockerfile
│   ├── .env
│   └── dags/orders_etl_dag.py
├── config/dashboard_queries.sql
├── data/source/orders_raw.csv
├── docker-compose.yml
├── docs/architecture.md
├── etl/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── run_pipeline.py
└── sql/init/001_init.sql
```

## Setup Instructions
1. Clone repository:
   ```bash
   git clone https://github.com/jr23ae3/airflow-etl.git
   cd airflow-etl
   ```

2. Start services:
   ```bash
   docker compose up -d postgres airflow adminer metabase
   ```

3. Open Airflow UI and trigger DAG:
- Airflow: http://localhost:8085
- Username: `admin`
- Password: `admin`

4. Optional manual ETL run without DAG:
   ```bash
   docker compose run --rm python-etl
   ```

5. Verify destination tables:
- `raw.orders`
- `analytics.daily_sales`
- `analytics.category_sales`

## Configurable Airflow Variables
- `etl_source_csv`
- `etl_postgres_host`
- `etl_postgres_port`
- `etl_postgres_db`
- `etl_postgres_user`
- `etl_postgres_password`
