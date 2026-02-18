#### Q-1 What is Apache Airflow ?
```bash
Airflow is an open-source workflow automation and orchestration tool that allows users to programmatically author, 
schedule, and monitor workflows as directed acyclic graphs (DAGs).
```

#### Q-2 What are DAGs in Airflow ?
```bash
DAGs (Directed Acyclic Graphs) are collections of tasks organized to reflect their relationships and dependencies.
Tasks have a direction — they run in a specific order.
Task A → Task B → Task C
A DAG in Airflow is:
A collection of tasks with defined dependencies that tells Airflow what to run, in what order, and on what schedule.

extract data→ transform data→ load into warehouse → email report

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="simple_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_function
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_function
    )

    extract >> transform
```

#### Q-3 How does Airflow handle dependencies between tasks ?
```bash
Dependencies are defined in DAGs using set_upstream() or set_downstream() methods 
set_downstream()
“Run this task BEFORE the given task.”
task1.set_downstream(task2)
task1 → task2

set_upstream()
“Run this task AFTER the given task.”
task2.set_upstream(task1)
task1 → task2

task1 >> task2
task2 << task1

or by using >> and << operators.
``` 

#### Q-4 What are the main components of Airflow ?
```bash
Scheduler: Orchestrates the execution of tasks.((Brain of Airflow))
What it does:
Scans DAG files
Checks schedule (cron, @daily, etc.)
Creates DAG runs
Decides which tasks are ready to run
Sends tasks to the Executor
Time matches schedule → Scheduler creates DAG run → Checks dependencies → Sends task to Executor

Executor: Handles the execution of tasks.
The Executor decides how tasks are executed.
It receives tasks from the Scheduler and assigns them somewhere to run.
Different types:
SequentialExecutor (local, single task)
LocalExecutor (parallel, same machine)
CeleryExecutor (distributed)
KubernetesExecutor (runs in pods)

Worker: Executes the tasks.
Workers are the machines/processes that actually run the tasks.

LocalExecutor → same machine
CeleryExecutor → distributed worker nodes
KubernetesExecutor → pods act as workers
Workers:
Run your Python, Bash, Spark, etc.
Report task status back

Web Server: Provides a user interface.
The Webserver provides the Airflow UI.
It shows:
DAG graph
Task status
Logs
Trigger buttons
Retry options

Metadata Database: Stores metadata.
Airflow stores EVERYTHING in its Metadata DB:
DAG runs
Task instances
Task status (success, failed, running)
Logs (optional depending setup)
Users
Variables
Connections
XComs
```

#### Q-5 What are Operators in Airflow ?
```bash
Operators are predefined tasks in Airflow that define what is executed. Types include BashOperator, 
PythonOperator, EmailOperator, etc.
Operators vs Sensors vs Hooks

Quick difference:
Operator → Performs an action
Sensor → Waits for something
Hook → Handles connection to external system

Example:
S3Hook → Connects to S3
S3Operator → Performs action on S3
S3KeySensor → Waits for file in S3

1. BashOperator : Runs a bash / shell command.

from airflow.operators.bash import BashOperator

task = BashOperator(
    task_id="run_shell",
    bash_command="echo Hello Airflow"
)
Run shell scripts,Trigger Spark jobs,Execute CLI tools,Move files,Run ETL scripts,

2. PythonOperator
Executes a Python function.

from airflow.operators.python import PythonOperator

def my_function():
    print("Hello from Python")
task = PythonOperator(
    task_id="run_python",
    python_callable=my_function
)

3. EmailOperator
Sends an email.

from airflow.operators.email import EmailOperator
email_task = EmailOperator(
    task_id="send_email",
    to="team@example.com",
    subject="Airflow Alert",
    html_content="<h3>Task Completed</h3>"
)
```

#### Q-6 What is a Task Instance in Airflow ?
```bash
A Task Instance is a specific run of a task for a specific DAG run and execution date.
A single run of a task for a specific DAG run and execution date.
Task → Defined in DAG file (static definition)
Task Instance → Runtime execution of that task for a specific DAG run

task1 = PythonOperator(task_id="extract", ...)

DAG → the workflow definition
Task → a step in the workflow (e.g., extract_flights)
Task Instance → that task running at a particular time
```

#### Q-7 Explain the difference between Operators and Sensors ?
```bash
Operator: Executes an action or operation.
Sensor: Waits for a condition to be met before executing downstream tasks.
```

#### Q-8 What are the different types of Executors in Airflow ?
```bash
SequentialExecutor
LocalExecutor
CeleryExecutor
KubernetesExecutor
```

#### Q-9 How does Airflow handle retries ?
```bash
Retries are configured using parameters like retries, retry_delay, and retry_exponential_backoff.
task = PythonOperator(
    task_id="my_task",
    python_callable=my_function,
    retries=3,
    retry_delay=timedelta(minutes=5)
)
running → failed → up_for_retry → queued → running
```

#### Q-10 How can you parameterize a DAG ?
```bash
By using dagrun.conf or Variable objects and passing arguments dynamically.
with DAG(
    dag_id="param_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    params={"file_path": "/data/default.csv"}
) as dag:

    task = PythonOperator(
        task_id="print_file",
        python_callable=my_function
    )
```

#### Q-11 What is XCom in Airflow ?
```bash
XCom (Cross-Communication) allows tasks to exchange small amounts of data during DAG runs.
It is commonly used to pass dynamic values such as IDs or file paths between tasks.
def task_b(**context):
    value = context["ti"].xcom_pull(task_ids="task_a")
    print(value)
```

#### Q-12 What are Airflow hooks ?
```bash
Hooks are interfaces to interact with external systems like databases, cloud services, etc.
They are used to manage connections and interact with external systems.
example : 
Query PostgreSQL
Upload file to S3
Call a REST API
Submit Spark job

from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id="my_postgres")
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT * FROM employees")
```

#### Q-13 How do you trigger a DAG manually ?
```bash
Using the Airflow UI, CLI, or API.
```

#### Q-14 What is the difference between depends_on_past and wait_for_downstream ?
```bash
depends_on_past: Ensures a task runs only if the previous instance of the same task succeeded.
Makes a task wait for its previous execution (previous DAG run) to succeed before running.

Example Scenario
extract → transform
If:
Jan 10 → transform fails
Jan 11 → DAG starts
If depends_on_past=True for transform:
Jan 11 transform will NOT run
Because Jan 10 failed.
It blocks future runs until the previous run succeeds.
task = PythonOperator(
    task_id="transform",
    python_callable=my_function,
    depends_on_past=True
)

wait_for_downstream: Ensures a task runs only if all downstream tasks from the previous instance succeeded.
Makes the current task wait until the downstream tasks of the previous DAG run finish before running.

Example Scenario :-
extract → transform → load
extract has wait_for_downstream=True

If:
Jan 10 load is still running
Jan 11 DAG starts
Jan 11 extract will WAIT
Until Jan 10 load finishes.

depends_on_past :-
Jan 10 transform → success
Jan 11 transform → allowed

Jan 10 transform → failed
Jan 11 transform → blocked

wait_for_downstream :-
Jan 10 extract → transform → load (still running)
Jan 11 extract → WAIT
```

#### Q-15 How is the Airflow Scheduler different from the Executor ?
```bash
The Scheduler determines what tasks to execute, while the Executor actually executes the tasks.

The Airflow Scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies 
are complete. It schedules jobs based on time or external triggers.
```

#### Q-16 How do you monitor workflows in Airflow ?
```bash
Using the web UI, logs, and metrics exposed through the monitoring tab.
```

#### Q-17 Explain the concept of TaskGroup ?
```bash
TaskGroup is a feature in Airflow that allows logical grouping of related tasks within a DAG for better 
organization and UI visualization. It does not create a separate DAG or affect execution behavior. It is 
a lightweight replacement for SubDAGs and helps structure complex workflows.
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    dag_id="taskgroup_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    with TaskGroup("user_pipeline") as user_group:

        extract = PythonOperator(
            task_id="extract",
            python_callable=extract_users
        )

        transform = PythonOperator(
            task_id="transform",
            python_callable=transform_users
        )

        load = PythonOperator(
            task_id="load",
            python_callable=load_users
        )

        extract >> transform >> load
```

#### Q-18 How do you manage Airflow configurations ?
```bash
Using the airflow.cfg file or environment variables.
```

#### Q-19 How do you deploy Airflow in production ?
```bash
Using a distributed setup with CeleryExecutor or KubernetesExecutor, along with proper monitoring and scaling.
```

#### Q-20 Explain Dynamic DAG generation ? 
```bash
Dynamically generating DAGs based on external inputs or configurations using Python logic.
```