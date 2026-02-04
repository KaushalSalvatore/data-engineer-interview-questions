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

#### Q-8 SequentialExecutor architecture diagram ?
![image_01](./images/image_04.png)

#### Q-9 LocalExecutor architecture diagram ?
![image_01](./images/image_05.png)


#### Q-10 KubernetesExecutor architecture diagram ?
![image_01](./images/image_07.png)


#### Q-11 CeleryExecutor  architecture diagram ?
![image_01](./images/image_08.png)


#### Q-12 How can you trigger DAGs in Airflow, and what are the different ways to do so ?
```bash

```

#### Q-13 What is Branching in Directed Acyclic Graphs (DAGs) ?
```bash
```

#### Q-14 How do you ensure that an Airflow workflow is idempotent ?
```bash
```

#### Q-15
```bash
```

#### Q-16
```bash
```

#### Q-17
```bash
```

#### Q-18
```bash
```

#### Q-19
```bash
```

#### Q-20 
```bash
```