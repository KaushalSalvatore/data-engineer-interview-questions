#### Q-1 Does Kinesis also have key, value concepts ? 
```bash
Yes, but not in exactly the same way as Kafka.

Kafka: Explicit Key-Value 

Key   = Customer_ID_101
Value = Customer details JSON

{
  "key": "101",
  "value": {
    "name": "John",
    "city": "Mumbai"
  }
}

Kinesis: Partition Key + Data

Partition Key
Data
Sequence Number

{
  "PartitionKey": "Customer_101",
  "Data": {
    "name": "John",
    "city": "Mumbai"
  }
}


kinesis.put_record(
    StreamName='customer_stream',
    Data=customer_json,
    PartitionKey='101'
)
```

#### Q-2 How Kinesis works as a distributed streaming platform ? How does message distribution happen across the partitions in kinesis ?
```bash
A Kinesis Data Stream is divided into Shards.

Producers
    |
    v
+------------------+
| Kinesis Stream   |
+------------------+
      |
+-----+-----+-----+
|Shard1|Shard2|Shard3|
+-----+-----+-----+
      |
Consumers

Each shard: Stores records in order, Has its own throughput limits, Can be consumed independently
This is what makes Kinesis distributed and scalable.

Distribution might look like:

Customer_101 -> Shard 1
Customer_202 -> Shard 3
Customer_303 -> Shard 2
Customer_101 -> Shard 1
Customer_202 -> Shard 3
```

#### Q-3 Does Athena detect the partition automatically or does it need some command ?
```bash
Athena does not automatically detect new partitions in traditional partitioned tables.

CREATE EXTERNAL TABLE sales (
    order_id BIGINT,
    amount DOUBLE
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
LOCATION 's3://my-bucket/sales/';

S3
 ↓
Glue Crawler
 ↓
Glue Catalog
 ↓
Athena

The crawler can automatically discover and register new partitions. However, Athena itself is not doing the 
discovery the crawler is.
```

#### Q-4 Retention period of data in Kinesis ?
```bash
| Retention Period | Availability       |
| ---------------- | ------------------ |
| 24 hours         | Default            |
| Up to 365 days   | Extended retention |


Record arrives at 10:00 AM

Retention = 7 days

Available until:
10:00 AM, 7 days later

aws kinesis increase-stream-retention-period \
  --stream-name your-stream-name \
  --retention-period-hours 216

Producers
     │
     ▼
Kinesis Data Stream
     │
     ├── Consumers (real-time processing)
     │
     └── Firehose / Lambda
             │
             ▼
            Amazon S3
             │
             ├── Lifecycle policies
             ├── Glacier Instant Retrieval
             ├── Glacier Flexible Retrieval
             └── Glacier Deep Archive
```

#### Q-5 what is a secondary index in DynamoDB ?
```bash
A Secondary Index in DynamoDB provides an alternate way to query data using attributes other than 
the table's primary key. DynamoDB supports two types: Global Secondary Indexes (GSIs), which can have 
different partition and sort keys from the base table, and Local Secondary Indexes (LSIs), which share 
the same partition key but use a different sort key. Secondary indexes improve query flexibility without 
requiring full table scans.

When designing a DynamoDB table, you first define the Primary Key. However, applications often need to 
query data in multiple ways. That's where Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI) 
come in.
```

#### Q-6 What is SQS in AWS ?
```bash
Buffer incoming data during traffic spikes.
Decouple producers from consumers so they can scale independently.
Trigger ETL or ELT jobs when new data arrives.
Retry failed processing without losing messages.
Build reliable event-driven data pipelines.

Customer Places Order
        │
        ▼
   Web Application
        │
        ▼
      AWS SQS
        │
 ┌──────┴─────────┐
 ▼                ▼
Inventory      Payment
Service        Service
        │
        ▼
 Notification Service
        │
        ▼
   Snowflake / S3


Multiple Services Read the Queue

Inventory Service - Reduces stock.

Payment Service - Charges the customer's credit card.

Shipping Service - Creates a shipment.

Email Service - Sends an order confirmation.
```

#### Q-7 design the pipeline in a such a way that whenever schema change one get notify through the glue service ?
```bash
I would use AWS Glue Crawlers to discover schemas and update the Glue Data Catalog. Event Bridge would monitor 
Glue table update events and trigger an SNS notification or Lambda function whenever a schema change occurs. 
The Lambda can compare the previous and current schema versions and send detailed alerts through email, Slack, 
or Teams. For streaming pipelines, I would use AWS Glue Schema Registry to enforce schema compatibility and 
notify teams of breaking changes before data is ingested.

S3 Landing Zone
       ↓
Glue Crawler
       ↓
Glue Catalog
       ↓
Event Bridge
       ↓
Lambda
       ↓
Compare Old vs New Schema
       ↓
SNS
       ↓
Email / Slack
```

#### Q-8  deploy lamdba code with diffrent ways ?  
```bash
Lambda Components

A Lambda function consists of:

Function Code (Python, Java, Node.js, etc.)
Handler (entry point, e.g., lambda_handler)
Runtime (Python, Java, .NET, etc.)
Trigger (S3, API Gateway, Kinesis, EventBridge, etc.)
Execution Role (IAM) (permissions to access AWS services)

def lambda_handler(event, context):
    print("Hello AWS Lambda")

    return {
        "statusCode": 200,
        "body": "Success"
    }

Ways to Deploy (Upload) a Lambda Function

1. ZIP File Upload (Most Common)

2. Upload from Amazon S3

3. Container Image (Docker)

4. CI/CD Pipeline

VS Code
    │
git push
    │
    ▼
GitHub
    │
    ▼
GitHub Actions
    │
    ▼
Deploy to AWS Lambda

What is a Lambda Layer :- A Lambda Layer is a reusable package of libraries or dependencies that can be shared 
across multiple Lambda functions.

Lambda Layer

      pandas
      numpy
      boto3
           │
   ┌───────┼────────┐
   ▼       ▼        ▼
Lambda1 Lambda2 Lambda3
```
 
#### Q-9 What is the Underlying Architecture of Amazon Redshift ?
```bash
The main architectural components are:

Client Applications
Leader Node
Compute Nodes
Node Slices
Managed Storage (for RA3 nodes)
Amazon S3 (Snapshots and Spectrum)
Columnar Storage
Query Optimizer and Execution Engine

                 BI Tools / SQL Client
         (Power BI, Tableau, JDBC, ODBC)
                        │
                        ▼
                +------------------+
                |   Leader Node    |
                |------------------|
                | SQL Parser       |
                | Optimizer        |
                | Query Planner    |
                | Result Merger    |
                +------------------+
                  /      |       \
                 /       |        \
                ▼        ▼         ▼
        +------------+ +------------+ +------------+
        | Compute 1  | | Compute 2  | | Compute 3  |
        +------------+ +------------+ +------------+
        | Slice 1    | | Slice 1    | | Slice 1    |
        | Slice 2    | | Slice 2    | | Slice 2    |
        | Slice 3    | | Slice 3    | | Slice 3    |
        +------------+ +------------+ +------------+
               │              │              │
               └──────────────┼──────────────┘
                              ▼
                    Columnar Data Storage
                              │
               RA3 Managed Storage / Local SSD
                              │
               Snapshots & Spectrum → Amazon S3

1. Client Layer
The client layer includes: Power BI,Tableau,SQL Workbench,DBeaver,JDBC/ODBC applications,Python applications
(The client sends SQL queries to the Leader Node.)

2. Leader Node
The Leader Node is the brain of the Redshift cluster. :
Responsibilities :- Accepts SQL queries,Authenticates users,Parses SQL,Validates SQL syntax,Creates an execution plan
,Optimizes the query,Distributes work to compute nodes,Collects partial results,Returns the final result to the client

The Leader Node does not store user table data. It manages coordination and query planning.

3. Compute Nodes
Compute nodes perform the actual data processing.
Responsibilities :- Store table data,Scan data,Filter rows,Perform joins,Aggregate data,Sort data,Execute SQL operations

If your cluster has 8 compute nodes, all 8 can process data simultaneously.

4. Slices
Each compute node is divided into Slices.
A slice is the smallest unit of parallel processing.

5. Massively Parallel Processing (MPP)
Data is distributed across multiple nodes.
Queries run on all nodes simultaneously.
Each node processes its own data.
The Leader Node combines the results.

6. Data Distribution
Redshift distributes data across compute nodes using distribution styles:
EVEN,KEY,ALL,AUTO
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

#### Q-17
```bash
```

#### Q-18
```bash
```

#### Q-19
```bash
```

#### Q-20 Your company has 500 TB of historical sales data in S3 and 10 TB of current data in Redshift. The business wants reports combining both datasets. What would you do ?
```bash
I would keep the frequently accessed 10 TB of current data inside Redshift for fast query performance. The 500 TB 
of historical data would remain in Amazon S3 to reduce storage costs. I would create external tables using Redshift 
Spectrum and query both datasets together in a single SQL statement. I would also store the S3 data in Parquet format 
and partition it by date to improve query performance and minimize the amount of data scanned.

Use Spectrum when:
Data is very large (tens or hundreds of terabytes).
Historical data is accessed occasionally.
You want to reduce Redshift storage costs.
Multiple analytics services need access to the same S3 data.
You want to build a data lake architecture.
```