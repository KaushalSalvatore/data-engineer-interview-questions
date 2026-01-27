#### Q-1 What is Apache Airflow ?
```bash
Airflow is an open-source workflow automation and orchestration tool that allows users to programmatically author, 
schedule, and monitor workflows as directed acyclic graphs (DAGs).
```

#### Q-2 What are DAGs in Airflow ?
```bash
DAGs (Directed Acyclic Graphs) are collections of tasks organized to reflect their relationships and dependencies.
```

#### Q-3 How does Airflow handle dependencies between tasks ?
```bash
Dependencies are defined in DAGs using set_upstream() or set_downstream() methods 
or by using >> and << operators.
```

#### Q-4 What are the main components of Airflow ?
```bash
Scheduler: Orchestrates the execution of tasks.
Executor: Handles the execution of tasks.
Worker: Executes the tasks.
Web Server: Provides a user interface.
Metadata Database: Stores metadata.
```

#### Q-5 What are Operators in Airflow ?
```bash
Operators are predefined tasks in Airflow that define what is executed. Types include BashOperator, 
PythonOperator, EmailOperator, etc.
```

#### Q-6 What is a Task Instance in Airflow ?
```bash
A single run of a task for a specific DAG run and execution date.
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
```

#### Q-10 How can you parameterize a DAG ?
```bash
By using dagrun.conf or Variable objects and passing arguments dynamically.
```

#### Q-11 What is XCom in Airflow ?
```bash
XCom (Cross-Communication) allows tasks to exchange small amounts of data during DAG runs.
```

#### Q-12 What are Airflow hooks ?
```bash
Hooks are interfaces to interact with external systems like databases, cloud services, etc.
They are used to manage connections and interact with external systems.
```

#### Q-13 How do you trigger a DAG manually ?
```bash
Using the Airflow UI, CLI, or API.
```

#### Q-14 What is the difference between depends_on_past and wait_for_downstream ?
```bash
depends_on_past: Ensures a task runs only if the previous instance of the same task succeeded.
wait_for_downstream: Ensures a task runs only if all downstream tasks from the previous instance succeeded.
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
TaskGroup is a feature that groups tasks visually in the DAG UI to improve readability.
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