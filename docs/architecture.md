# Airflow ETL Architecture

```mermaid
flowchart TB
    A[Source CSV] --> B[Airflow DAG Scheduler]
    B --> C[Extract Operator]
    C --> D[Transform Operator]
    D --> E[Load Operator]
    E --> F[Quality Check Operator]
    F --> G[(PostgreSQL raw + analytics)]
    G --> H[Dashboard / BI]

    B -. Variables .-> C
    B -. Variables .-> D
    B -. Variables .-> E
    C -. Logs .-> I[Airflow Logs]
    D -. Logs .-> I
    E -. Logs .-> I
    F -. Logs .-> I
```

## Components
- Scheduling: Daily DAG schedule at 02:00
- Operators: PythonOperator for ETL + BashOperator for run logs
- Variables: Runtime source and database configuration
- Logging: Task-level logs in Airflow task logs
- Retries: Default retries on ETL/quality tasks
- Storage: Postgres schemas `raw` and `analytics`
