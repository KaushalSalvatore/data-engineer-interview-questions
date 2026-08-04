#### Q-1 clear state in airflow how to use it ? 
```bash
Real-world example

Imagine an ETL pipeline:

Extract from S3
        ↓
Transform using Spark
        ↓
Load into Snowflake

The Transform task fails because the Spark cluster is unavailable.

Restart or fix the Spark cluster.
Open Airflow.
Select the failed Transform task.
Click Clear (optionally include downstream tasks).
The scheduler reruns Transform, and if it succeeds, Load into Snowflake executes afterward.

example :-

airflow dags clear my_dag \
    --start-date 2026-07-01 \
    --end-date 2026-07-01

Difference Between Clear, Retry, and Mark Success :-

| Action           | Description                                                    | Typical Use                                |
| ---------------- | -------------------------------------------------------------- | ------------------------------------------ |
| **Retry**        | Automatically reruns after a failure if retries are configured | Temporary failures                         |
| **Clear**        | Resets task state to `None` so the scheduler can run it again  | After fixing issues or rerunning pipelines |
| **Mark Success** | Marks the task as successful without executing it              | Skip a task intentionally                  |
```

#### Q-2 how to trigger snowflake job in airflow ?
```bash
pip install apache-airflow-providers-snowflake (Install the Snowflake Provider)

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

Airflow
   │
   ▼
SQLExecuteQueryOperator
   │
   ▼
Snowflake

with DAG(
    dag_id="snowflake_sql_job",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    run_sql = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="snowflake_default",
        sql="""
            CREATE TABLE IF NOT EXISTS EMPLOYEE(
                ID INT,
                NAME STRING
            );
        """,
    )
```

#### Q-3
```bash
```

#### Q-4
```bash
```

#### Q-5
```bash
```

#### Q-6
```bash
```

#### Q-7
```bash
```

#### Q-8
```bash
```

#### Q-9
```bash
```
#### Q-10
```bash
```

#### Q-11
```bash
```

#### Q-12
```bash
```
#### Q-13
```bash
```

#### Q-14
```bash
```

#### Q-15
```bash
```
#### Q-16
```bash
```

#### Q-2
```bash
```

#### Q-3
```bash
```
