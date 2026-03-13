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
```

#### Q-4 What exactly is the Lambda architecture of AWS ?
```bash
```

#### Q-5 How does S3 encryption work ?
```bash
```

#### Q-6 What is Cross-Region Replication (CRR) in S3 ?
```bash
```

#### Q-7 What is Amazon EMR ?
```bash
```

#### Q-8 How does Amazon EMR handle data processing ?
```bash
```

#### Q-9 What are the main components of an EMR cluster ?
```bash
```

#### Q-10 What file systems are supported by Amazon EMR ?
```bash
```

#### Q-11 What is the difference between core nodes and task nodes in an EMR cluster ?

```bash
```

#### Q-12 What are bootstrap actions in EMR ?
```bash
```

#### Q-13 What monitoring options are available for Amazon EMR ?
```bash
```

#### Q-14 Can you run real-time streaming applications on EMR ?
```bash
```

#### Q-15 What are the common use cases of Amazon EMR ?
```bash
```

#### Q-16 What is Amazon EMR’s Step Debugging feature ?
```bash
```

#### Q-17 How does Amazon EMR handle job scheduling?
```bash
```

#### Q-18 What is the difference between AWS EMR and Glue?
```bash
```

#### Q-19 Explain the Difference Between On-Demand and Spot Instances in EMR ?
```bash
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