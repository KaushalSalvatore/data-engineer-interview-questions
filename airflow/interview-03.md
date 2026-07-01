#### Q-1 What Executor will you use to test multiple jobs at a low scale ?
```bash
Local Executor is ideal for testing multiple jobs in parallel for performing tasks for a small-scale production
environment. The Local Executor runs the tasks on the same node as the scheduler but on different processors. 
There are other executors as well who use this style while distributing the work. Like, Kubernetes Executor would 
also use Local Executor within each pod to run the task.
```

#### Q-2 If we want to exchange large amounts of data, what is the solution to the limitation of XComs ?
```bash
Since Airflow is an orchestrator tool and not a data processing framework, if we want to process large gigabytes 
of data with Airflow, we use Spark (which is an open-source distributed system for large-scale data processing) 
along with the Airflow DAGs because of all the optimizations that It brings to the table.
```

#### Q-3 What are the pros and cons of SequentialExecutor ?
```bash
Pros:
It's simple and straightforward to set up.
It's a good way to test DAGs while they're being developed.

Cons: 
It isn't scalable. It is not possible to perform many tasks at the same time. Unsuitable for use in production
```

#### Q-4 What are the pros and cons of LocalExecutor ?
```bash
Pros:
Able to perform multiple tasks.
Can be used to run DAGs during development.

Cons:
The product isn't scalable.
There is only one point of failure.
Unsuitable for use in production.
```

#### Q-5 What are the pros and cons of CeleryExecutor ?
```bash
Pros:
It allows for scalability.
Celery is responsible for managing the workers. Celery creates a new one in the case of a failure.

Cons:
Celery requires RabbitMQ/Redis for task queuing, which is redundant with what Airflow already supports.
The setup is also complicated due to the above-mentioned dependencies.
```

#### Q-6 What are the pros and cons of KubernetesExecutor ?
```bash
Pros:
It combines the benefits of CeleryExecutor and LocalExecutor in terms of scalability and simplicity.
Fine-grained control over task-allocation resources. At the task level, the amount of CPU/memory needed 
can be configured.

Cons:
Airflow is newer to Kubernetes, and the documentation is complicated.
```

#### Q-7 How to define a workflow in Airflow?
```bash
from Airflow.models import DAG
from airflow.utils.dates import days_ago
​
args = {
'start_date': days_ago(0),
}
​
dag = DAG(
dag_id='bash_operator_example',
default_args=args,
schedule_interval='* * * * *',
)
```
#### Q-8 How can you trigger DAGs in Airflow, and what are the different ways to do so ?
```bash
✅ 1. Scheduled Trigger (Automatic)
Defined using schedule_interval in the DAG
Airflow triggers it automatically based on time
👉 Example:
Daily, hourly, cron jobs
🗣️ Interview line:
“DAGs can be triggered automatically using a schedule interval like cron expressions.”

✅ 2. Manual Trigger (UI)
Triggered from the Airflow web UI
Click on “Trigger DAG”
🗣️ Interview line:
“We can manually trigger DAGs from the Airflow UI for testing or ad-hoc runs.”

✅ 3. CLI Trigger
Using command line
👉 Example:
airflow dags trigger dag_id

✅ 4. External Trigger (TriggerDagRunOperator)
One DAG triggers another DAG
👉 Used in:
Pipeline chaining
🗣️ Interview line:
“Using TriggerDagRunOperator, one DAG can trigger another DAG as part of workflow orchestration.”

✅ 5. Sensors / Event-Based Trigger
DAG waits for an event (file, DB update, etc.)
👉 Example:
File arrives in S3
Database record appears
🗣️ Interview line:
“Sensors allow DAGs to be triggered based on external events like file arrival or database changes.”
```

#### Q-9 What is Branching in Directed Acyclic Graphs (DAGs) ?
```bash
Branching allows a workflow to decide which task(s) to run next based on some logic.

How it works
A special task decides which path to follow
Only the selected branch runs
Other branches are skipped

BranchPythonOperator
It returns the task_id(s) of the next task(s) to execute.

Example Scenario
Let’s say:
If data is valid → process data
If data is invalid → send alert

from airflow.operators.python import BranchPythonOperator

def choose_path(**kwargs):
    if kwargs['ti'].xcom_pull(task_ids='check_data'):
        return 'process_data'
    else:
        return 'send_alert'
```

#### Q-10 How do you ensure that an Airflow workflow is idempotent ?
```bash
Idempotent workflow means: Running the same DAG multiple times produces the same result without duplication or 
inconsistency.

Why it matters
Airflow retries tasks on failure
DAGs can be re-run manually
Backfills may execute past runs
👉 So your pipeline must not corrupt data or duplicate results

✅ How to Ensure Idempotency

1. Avoid Duplicate Writes
Use UPSERT (merge) instead of INSERT
Use primary keys / unique constraints

🗣️ Say in interview:
“I ensure idempotency by avoiding duplicate inserts and using upserts or deduplication logic.”

2. Use Partitioned Data
Store data by date (e.g., dt=2026-03-30)
Each DAG run processes only its partition

🗣️ Say in interview:
“I design pipelines to process data partition-wise so reruns only affect a specific partition.”
```

#### Q-11 how do you pass data between task and dag in airflow ? 
```bash
✅ Main Ways to Pass Data Between Tasks

1. XCom (Cross-Communication) — Most Important
👉 Used to pass small amounts of data between tasks
Push data from one task
Pull data in another task

def push_data(**context):
    context['ti'].xcom_push(key='value', value=42)

def pull_data(**context):
    value = context['ti'].xcom_pull(task_ids='push_task', key='value')

2. TaskFlow API (Modern & Cleaner)
👉 Automatically uses XCom behind the scenes
from airflow.decorators import task

@task
def task1():
    return 100

@task
def task2(value):
    print(value)

4. External Storage (Best for Large Data)
👉 Recommended for real-world use
Store data in:
S3 / GCS
Database
File system
Pass only reference (path, ID) via XCom

✅ Main Ways to Pass Data Between dags 

data can be passed between different DAGs in Airflow. The recommended approach is using TriggerDagRunOperator 
with conf or external storage like S3 or databases. XCom can be used but is not ideal across DAGs due to tight
coupling.

1. TriggerDagRunOperator + conf (Best Airflow Way)
One DAG triggers another DAG
Pass data using conf

TriggerDagRunOperator(
    task_id='trigger_dag2',
    trigger_dag_id='dag2',
    conf={"key": "value"}
)

2. XCom (with limitations)
xcom_pull(dag_id='dag1', task_ids='task1')
```

#### Q-12 can we run two task and dag paralley  
```bash
-> Tasks run in parallel when they are independent and don’t have upstream/downstream dependencies.

-> multiple DAGs can run simultaneously
Airflow scheduler can trigger multiple DAGs at once
Depends on system resources and configuration

🗣️ Interview line:
“Multiple DAGs can run in parallel as long as the scheduler and executor have enough resources.”

If using Sequential Executor → ❌ no parallelism
If using Local/Celery/Kubernetes Executor → ✅ parallelism supported

Yes, both tasks and DAGs can run in parallel in Airflow. Tasks run in parallel when they don’t have dependencies, 
and DAGs run in parallel based on scheduler capacity and configuration like parallelism and max_active_runs. The 
level of parallelism also depends on the executor being used.
```

#### Q-13 how do you debug failures in airflow ?
```bash
1. Check Task Logs (Most Important)
Go to Airflow UI → Click on failed task → View Logs
👉 Look for:
Error message
Stack trace
Failed query / API call

🗣️ Interview line:
“I start by checking task logs in the Airflow UI to identify the root cause.”

2. Check Task Dependencies
Ensure upstream tasks succeeded
Check if data/input is available


3. Check Scheduler & Worker Logs
Scheduler logs → DAG not triggering
Worker logs → execution failure

4. Check External Systems
Database connection
API availability
File paths (S3, local, etc.)

🗣️: “I verify external dependencies like databases or APIs, since many failures originate there.”

5. Review Retries & Alerts
Check retry attempts
Look at email/Slack alerts

6. Use XCom (if needed)
Inspect intermediate values passed between tasks
```

#### Q-14 if create two dags in airflow and  can i call one task from another dag ? 
```bash
You cannot directly call a task from another DAG

1. Trigger the Other DAG (Most Common)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger = TriggerDagRunOperator(
    task_id="trigger_dag2",
    trigger_dag_id="dag2"
)
```

#### Q-15
```bash
```

#### Q-16
```bash
```

#### Q-17 SequentialExecutor architecture diagram ?
![image_01](../images/image_04.png)

#### Q-18 LocalExecutor architecture diagram ?
![image_01](./images/image_05.png)

#### Q-19 KubernetesExecutor architecture diagram ?
![image_01](./images/image_07.png)

#### Q-20 CeleryExecutor  architecture diagram ?
![image_01](./images/image_08.png)
