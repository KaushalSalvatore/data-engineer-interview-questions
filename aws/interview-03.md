#### Q-1 What is AWS Lambda Layers, and how can they be used to share code and libraries across multiple functions ?
```bash
A Lambda Layer is a ZIP archive that contains libraries, runtime dependencies, or custom code.
Typical contents include:
Python libraries (NumPy, Pandas)
Node.js packages
Custom utility modules
Shared configuration files
SDK extensions

                +----------------+
                | Lambda Layer   |
                |  - pandas      |
                |  - numpy       |
                |  - utils.py    |
                +--------+-------+
                         |
        -------------------------------------
        |                |                 |
   Lambda A         Lambda B          Lambda C
  ETL Job         API Handler       Data Cleaner
```

#### Q-2 Explain the concept of AWS Step Functions and how they can be used in conjunction with Lambda functions to build complex workflows ?
```bash
How Step Functions Work with Lambda
Lambda functions handle individual tasks, while Step Functions handle workflow coordination.

Step Functions (Orchestrator)
        |
        |---- Lambda 1 (Extract Data)
        |
        |---- Lambda 2 (Transform Data)
        |
        |---- Lambda 3 (Load Data)

Types of Step Functions

Standard Workflows
Best for:
long-running workflows
complex orchestration

Express Workflows
Best for:
high-volume event processing
streaming pipelines
```

#### Q-3 What are the constraints that AWS lambda function imposes ?
```bash
1️⃣ Execution Time Limit
A Lambda function has a maximum execution time of 15 minutes.
Minimum timeout: 1 second
Maximum timeout: 900 seconds (15 minutes)

Long-running jobs should use services like:
AWS Step Functions
AWS Batch

2️⃣ Memory Limit
Lambda memory allocation ranges from:
128 MB → 10,240 MB (10 GB)

128 MB  → low CPU
3008 MB → higher CPU
10 GB   → highest CPU

3️⃣ Deployment Package Size
ZIP Deployment
50 MB max (compressed upload)
250 MB max (unzipped including layers)

Container Image Deployment
Up to 10 GB

Used for:
large dependencies
ML libraries
complex runtimes

4️⃣ Concurrency Limits
Lambda runs functions concurrently.
Default account concurrency limit:
1000 concurrent executions

If exceeded:
new requests get throttled

Solutions:
request limit increase
reserved concurrency
```

#### Q-4 What exactly is the Lambda architecture of AWS ?
```bash
1️⃣ Core Idea of Lambda Architecture
Lambda Architecture processes data using two parallel paths:
Batch processing layer → accurate but slower
Speed (real-time) layer → fast but approximate

2️⃣ Three Main Layers
1. Batch Layer

Responsibilities:
Store all raw data
Perform large-scale batch computations
Generate batch views

Typical AWS services:
Amazon S3
AWS Glue
Amazon EMR

2. Speed Layer (Real-Time Layer)
Responsibilities:
process streaming data
produce low-latency updates

Typical AWS services:
Amazon Kinesis
AWS Lambda

3. Serving Layer
The serving layer combines results from both batch and speed layers and provides them to applications.

Typical services:
Amazon Redshift
Amazon DynamoDB
Amazon Athena
```

#### Q-5 How does S3 encryption work ?
```bash
Upload Object
      |
      v
S3 encrypts using AES-256
      |
      v
Encrypted object stored

1. Encryption at Rest
Data is encrypted when stored in S3.

2. Encryption in Transit
Data is encrypted while moving between client and S3.

1. SSE-S3 (S3 Managed Keys)
S3 automatically:

encrypts data
manages encryption keys
rotates keys
Uses AES-256 encryption.
```

#### Q-6 What is Cross-Region Replication (CRR) in S3 ?
```bash
CRR replicates data asynchronously from a source bucket to a destination bucket located in another AWS region.

How CRR Works
The replication process follows these steps:
A file is uploaded to the source bucket
S3 detects the replication rule
The object is copied to the destination bucket in another region
Metadata and object versions are replicated

User Upload
     |
     v
Source Bucket
     |
     v
Replication Rule
     |
     v
Destination Bucket (different region)
```

#### Q-7 What is Amazon EMR ?
```bash
Amazon EMR (Elastic MapReduce) is a managed big data processing service from AWS used to run large-scale 
data processing frameworks like Hadoop, Spark, Hive, and Presto without managing the underlying infrastructure.

Core Idea of Amazon EMR
Instead of setting up your own big data cluster, EMR automatically provisions and manages a cluster of 
EC2 machines to run big data jobs.

Typical workflow:
Data → Processing Framework → Results

Frameworks Supported by EMR
| Framework      | Purpose                     |
| -------------- | --------------------------- |
| Apache Spark   | Fast distributed processing |
| Hadoop         | Batch processing            |
| Hive           | SQL queries on big data     |
| Presto / Trino | Interactive analytics       |
| HBase          | NoSQL database              |
| Flink          | Stream processing           |

Raw Data
   |
   v
Amazon S3
   |
   v
Spark Job on EMR
   |
   v
Processed Data
   |
   v
Amazon Redshift / S3
```

#### Q-8 How does Amazon EMR handle data processing ?
```bash
Amazon EMR handles data processing by creating a distributed cluster of compute nodes and running big-data 
frameworks (like Spark or Hadoop) to process large datasets in parallel. Instead of one machine processing 
all the data, EMR splits the work across many machines, which makes processing faster and scalable.

Dataset = 1 TB
Node 1 → process 200 GB
Node 2 → process 200 GB
Node 3 → process 200 GB
Node 4 → process 200 GB
Node 5 → process 200 GB
```

#### Q-9 What are the main components of an EMR cluster ?
```bash
Cluster Architecture Example
            Master Node
                 |
        ----------------------
        |                    |
     Core Node           Core Node
        |                    |
     Task Node           Task Node

1️⃣ Master Node
The Master Node is the control center of the cluster.

Responsibilities
Manages the cluster
Schedules and coordinates jobs
Tracks task progress
Monitors node health
Manages resource allocation

It runs cluster management services such as:
Resource manager
Job scheduler

2️⃣ Core Nodes
Core nodes are worker nodes that store data and run processing tasks.

Responsibilities
Store data in HDFS
Execute processing tasks
Support distributed computing

They are responsible for both storage and computation.

3️⃣ Task Nodes
Task nodes are optional worker nodes used only for computation.

Responsibilities
Execute processing tasks
Do not store data
Increase compute capacity

These nodes help scale processing power without increasing storage.
```

#### Q-10 What file systems are supported by Amazon EMR ?
```bash
1️⃣ HDFS (Hadoop Distributed File System)
2️⃣ EMRFS (EMR File System)
EMRFS is an EMR-specific file system that allows the cluster to access data stored in:
Amazon S3
It acts as a bridge between Hadoop/Spark and S3.
3️⃣ Local File System
```

#### Q-11 What is the difference between core nodes and task nodes in an EMR cluster ?
```bash
1️⃣ Core Nodes
Core nodes are worker nodes that store data and process tasks.

Responsibilities
Store data using HDFS
Execute data processing tasks
Maintain data replication for fault tolerance
Participate in distributed storage

2️⃣ Task Nodes

Task nodes are optional worker nodes used only for computation.

Responsibilities
Execute processing tasks
Increase cluster compute capacity
Do not store data

Core nodes handle both storage and computation, while task nodes handle computation only.
```

#### Q-12 What are bootstrap actions in EMR ?
```bash
Sometimes the default EMR cluster does not include everything you need. Bootstrap actions let you:
Install additional libraries
Configure environment variables
Download scripts or dependencies
Set up custom tools

Example Use Cases
Install pandas
Install numpy
Install scikit-learn
Install Kafka client
Install monitoring tools
Install security agents
```

#### Q-13 What monitoring options are available for Amazon EMR ?
```bash
1️⃣ Amazon CloudWatch
2️⃣ CloudWatch Logs
3️⃣ Amazon S3 Log Storage
4️⃣ EMR Console Monitoring
```

#### Q-14 Can you run real-time streaming applications on EMR ?
```bash
Yes, Amazon EMR can run real-time streaming applications using frameworks such as Apache Spark Streaming,
Apache Flink, and Apache Storm. These frameworks allow EMR clusters to process continuous streams of data 
from sources like Kafka or Amazon Kinesis and perform real-time analytics or transformations before storing 
the results in services such as Amazon S3 or data warehouses.


```

#### Q-15 What are the common use cases of Amazon EMR ?
```bash
1️⃣ Big Data ETL Processing
2️⃣ Log Processing and Clickstream Analytics
3️⃣ Machine Learning and Data Science
4️⃣ Data Lake Processing
5️⃣ Real-Time Stream Processing
6️⃣ Interactive Data Analytics
```

#### Q-16 What is Amazon EMR’s Step Debugging feature ?
```bash
1️⃣ What Is an EMR Step?
In EMR, a step is a unit of work executed on the cluster.

Examples of steps:
running a Spark job
executing a Hive query
running a Hadoop MapReduce program

EMR Cluster
     |
     ├─ Step 1: Spark Job
     ├─ Step 2: Hive Query
     └─ Step 3: Data Processing Script

2️⃣ How Step Debugging Works
When Step Debugging is enabled, EMR automatically:
Captures logs from cluster nodes
Uploads logs to
Amazon S3
Runs debugging tools to analyze the failure
Provides diagnostic information in the EMR console

Job Step Fails
      |
      v
Logs Collected
      |
      v
Stored in S3
      |
      v
Debugging Tools Analyze Logs

3️⃣ Example Failure Scenario

Spark Job
     |
     v
Out-of-Memory Error
     |
     v
Step Debugging Collects Logs
     |
     v
Logs stored in S3 → Review failure cause
```

#### Q-17 How does Amazon EMR handle job scheduling?
```bash
1️⃣ EMR Steps (Basic Job Scheduling)
Examples:
Spark application
Hive query
Hadoop MapReduce job

2️⃣ YARN Resource Manager
YARN is part of the Hadoop ecosystem and is responsible for:
allocating CPU and memory
scheduling jobs
managing cluster resources

3️⃣ Framework-Level Scheduling
Example with Spark:
Spark Driver submits jobs
Jobs are split into stages
Stages are split into tasks
Tasks are distributed to worker nodes
```

#### Q-18 What is the difference between AWS EMR and Glue?
```bash
EMR → more control and flexibility
Glue → serverless and easier to manage

EMR

With EMR you manage a cluster of machines running on
Amazon EC2.
You must:
choose instance types
configure cluster size
manage scaling

Glue
Glue is serverless, meaning AWS automatically manages the infrastructure.
You simply:
Write ETL script → Run Glue Job
```

#### Q-19 Explain the Difference Between On-Demand and Spot Instances in EMR ?
```bash
1️⃣ On-Demand Instances
On-Demand Instances are the standard EC2 instances you pay for per second or hour with no long-term 
commitment.

Characteristics
Always available when capacity exists
No interruptions from AWS
More expensive than Spot Instances
Best for critical workloads

2️⃣ Spot Instances

Spot Instances allow you to use unused EC2 capacity at a much lower price.

Characteristics
Up to 70–90% cheaper than On-Demand
Can be terminated by AWS with short notice when capacity is needed elsewhere
Best for fault-tolerant workloads
```

#### Q-20 What are AWS Glue Crawlers and what do they do ?
```bash
Raw Data → Crawler → Schema → Data Catalog Table
An AWS Glue Crawler:
Connects to a data source
Reads the data structure
Infers the schema
Creates or updates tables in the AWS Glue Data Catalog

What Can It Crawl ?
It can scan:
Amazon S3 (most common)
Amazon RDS
Amazon Redshift
JDBC data sources

Why Glue Crawlers Are Useful
1️⃣ Automatic Schema Discovery
2️⃣ Schema Evolution
3️⃣ Keeps Data Catalog Updated

How It Works (Step-by-Step)
Create Crawler
Choose data source (e.g., S3 path)
Assign IAM role
Choose database in Data Catalog
Run crawler

Crawlers do NOT move or transform data
They only create metadata.
Actual transformations happen in:
Glue Jobs
Spark jobs
EMR
Lambda
```     