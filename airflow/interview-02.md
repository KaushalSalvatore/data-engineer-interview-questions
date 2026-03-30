#### Q-1 What is the role of Airflow Plugins ?
```bash
Plugins extend Airflow functionalities like creating custom operators, sensors, hooks, etc.
```

#### Q-2 What are DAG Run states ?
```bash
Running
Success
Failed
```

#### Q-3 How would you optimize DAG performance ?
```bash
Avoid large DAG files.
Use parallelism and concurrency.
Offload heavy computations.
```

#### Q-4 How do you set up a custom Operator ?
```bash
An operator in Airflow represents a single task, or a unit of work, within a DAG. Each operator determines 
what actually happens in a task.
Subclass the BaseOperator class and define the execute() method.
```

#### Q-5 What is the Airflow Meta Database ?
```bash
The Airflow Meta Database is where Airflow stores its metadata. This includes information about the status of 
tasks, DAGs, variables, connections, and historical data about the workflow execution.
```

#### Q-6 How do you use Python scripts in Airflow?
```bash
Python scripts in Airflow are used to define the logic of operators, DAGs, and plugins. They are written as standard 
Python files and allow for extensive customization and control over your workflows.
```

#### Q-7 
```bash
```

#### Q-8 What is the purpose of Airflow Variables ?
```bash
Airflow Variables are used to store dynamic values that can be accessed and used in DAGs and tasks. They offer a 
way to avoid hard-coding and to manage configuration settings.
```

#### Q-9 How do you handle errors and retries in Airflow ?
```bash
Errors and retries in Airflow are handled by setting the retries and retry_delay parameters in task definitions. 
Airflow will automatically retry a failed task according to these settings.
```

#### Q-10 Can you describe a scenario where you used the CeleryExecutor in Airflow ?
```bash
The CeleryExecutor is used in distributed environments where you need to run tasks on multiple machines. I used it 
in a project where tasks were resource-intensive and required to be distributed across different nodes to balance the 
load.
```

#### Q-11 What is the difference between a DAG and a task in Airflow ?
```bash
In Airflow, a DAG is a collection of tasks organized with dependencies and relationships to define a workflow. A task, 
on the other hand, is a single operation or step within a DAG, defined by an operator.
```

#### Q-12 How do you ensure high availability in Airflow ?
```bash
High availability in Airflow can be achieved by setting up a multi-node cluster with a database like PostgreSQL or 
MySQL that supports high availability and using a distributed executor like the CeleryExecutor.
```

#### Q-13 How to Improve Apache Airflow’s Scheduler Efficiency ?
```bash
Tuning Scheduler Environment Variables
We can adjust the following variables:

scheduler.min_file_process_interval – Defines how frequently the scheduler scans DAG files (default: 30s). We can increase 
it to 300s to reduce parsing frequency.

core.min_serialized_dag_update_interval – Controls how often DAG states are updated in the Airflow database (default: 30s). 
We can raise it to 300s to decrease the scheduler’s workload.

core.sql_alchemy_pool_size – Specifies the number of database connections in the pool. We can increase it from 5 to 25 to 
shift the load from CPU-bound tasks to database queries.

scheduler.scheduler_idle_sleep_time – Determines how long the scheduler sleeps between loops (default: 1s). We can increase 
it to 5s to reduce CPU strain.
```

#### Q-14 Explain the difference between LocalExecutor, CeleryExecutor, and KubernetesExecutor ?
```bash
LocalExecutor runs tasks in parallel on the same machine. It’s good for small setups. 

CeleryExecutor distributes tasks across multiple workers using Celery, making it suitable for medium to large teams. 

KubernetesExecutor runs each task in its own Kubernetes Pod, giving the best scalability and isolation for production environments.
```

#### Q-15 How do you monitor and debug failed DAG runs in production ?
```bash
I usually start with the Airflow UI grid view. Failed tasks are marked red. Clicking on them gives logs that show the 
stack trace or error message. If retries don’t solve it, I check system-level issues like permissions, missing 
connections, or resource limits. For critical jobs, I also set alerts through callbacks or integrations like PagerDuty 
or Slack.
```

#### Q-16 What is the role of ExternalTaskSensor ?
```bash
ExternalTaskSensor waits for a task in another DAG to finish before running the current task. It’s useful when multiple 
DAGs depend on each other, such as one DAG loading raw data and another processing it afterward.
```

#### Q-17 Why don't we use Variables instead of Airflow XComs, and how are they different ?
```bash
An XCom is identified by a "key," "dag id," and the "task id" it had been called from. These work just like variables 
but are alive for a short time while the communication is being done within a DAG. In contrast, the variables are global 
and can be used throughout the execution for configurations or value sharing.

There might be multiple instances when multiple tasks have multiple task dependencies. defining a variable for each instance 
and deleting them at quick successions would not be suitable for any process's time and space complexity.
```

#### Q-18 What are the states a Task can be in? Define an ideal task flow ?
```bash
Just like the state of a DAG (directed acyclic graph) being running is called a "DAG run", the tasks within that 
dag can have several tasks instances. they can be:

none: the task is defined, but the dependencies are not met.

scheduled: the task dependencies are met, has got assigned a scheduled interval, and are ready for a run.

queued: the task is assigned to an executor, waiting to be picked up by a worker.

running: the task is running on a worker.

success: the task has finished running, and got no errors.

shutdown: the task got interrupted externally to shut down while it was running.

restarting: the task got interrupted externally to restart while it was running.

failed: the task encountered an error.

skipped: the task got skipped during a dag run due to branching (another topic for airflow interview, will cover branching 
some reads later)

upstream_failed: An upstream task failed (the task on which this task had dependencies).

up_for_retry: the task had failed but is ongoing retry attempts.

up_for_reschedule: the task is waiting for its dependencies to be met (It is called the "Sensor" mode).

deferred: the task has been postponed.

removed: the task has been taken out from the DAG while it was running.

Ideally, the expected order of tasks should be : none -> scheduled -> queued -> running -> success.
```

#### Q-19 How does airflow communicate with a third party (S3, Postgres, MySQL) ?
```bash
Airflow uses Hooks (a high-level interface) to interact with third-party systems, which enables its connection to 
external APIs and databases like S3, GCS, MySQL, and Postgres.
```

#### Q-20 Demonstrate the use of macros in a DAG task in python ? 
```bash
Below is a code for a task printing the task execution date and print it after adding 2 days to it.

task_A = BashOperator(
 task_id="execution_date",
 bash_command="echo 'execution date : {{ ds }} ds_add: {{ macros.ds_add(ds, 2) }}'"
)
```