#### Q-1 what is difference in distributed process and storage ?
```bash
Distributed Processing : Distributed processing means splitting a computational task across multiple machines 
(nodes) that work together to process data or perform calculations.

Example:

Running parts of a large simulation on different servers.
A MapReduce job where:
Map tasks process chunks of data in parallel.
Reduce tasks aggregate the results.

Examples of technologies:
Apache Spark, Hadoop MapReduce

Distributed Storage : Distributed storage means splitting and storing data across multiple physical or virtual 
storage systems (nodes, disks, or servers), often with replication for reliability.

Example:

A file is split into blocks stored on different servers.
Cloud object storage like Amazon S3 or Google Cloud Storage.

Benefits:

High availability and fault tolerance (data is replicated).
Scalability (add more storage nodes to handle more data).
Improved access speed (data can be read from the nearest node).

Examples of technologies:

HDFS (Hadoop Distributed File System), Amazon S3, Google File System, Ceph, Cassandra 
(for distributed databases).
```

#### Q-2 Difference in RDD , DataSet , DataFrame ?
```bash
RDD (Resilient Distributed Dataset)
RDD is a low-level distributed collection of objects with no schema and no optimization, so it’s more flexible 
but slower.
DataFrame is a higher-level abstraction with schema, optimized by Catalyst and Tungsten, which makes it much 
faster and easier to use.
Dataset combines the benefits of RDD and DataFrame by providing schema and compile-time type safety, but it’s 
available only in Scala and Java.
In real projects, we mostly use DataFrames because they give the best performance with less code.

Which one do you use and why ?
I mostly use DataFrames because they are optimized, easy to write, and work well with Spark SQL.
I use RDD only when I need low-level transformations or unstructured data.
Dataset is useful in Scala when type safety is required.”
```

#### Q-3 AWS step function or Airflow which i have to choice for scheduling jobs ?
```bash
The choice between AWS Step Functions and Airflow depends on the workload and environment.
If the jobs are AWS-native, event-driven, and relatively simple workflows, I prefer AWS Step Functions because 
it’s serverless, highly reliable, and requires minimal maintenance.
If the workflows are complex, involve many dependencies, conditional logic, retries, or span across multiple systems 
and clouds, then Airflow is a better choice because it provides more flexibility and rich scheduling features.

Step Functions are great for orchestrating AWS services.
Airflow excels at complex data pipelines and scheduling logic.
```

#### Q-4 What is Dimensional Modeling ?  
```bash
Dimensional modeling is a data-warehouse design technique used to organize data for analytics and reporting.
It structures data into fact tables that store measurable business metrics and dimension tables that store 
descriptive attributes.
The goal is to make queries simple, fast, and easy for business users to understand.

1. Fact Table
Fact tables store quantitative measures like sales amount, quantity, or revenue, along with foreign keys to 
dimensions.

Examples:
sales_amount
order_count
profit

2️. Dimension Table
Dimension tables store descriptive attributes used for filtering and grouping.

Examples:
Customer (name, age, city)
Product (category, brand)
Date (day, month, year)
```

#### Q-5 Design Dimensional modeling for social media ? 
```bash
For social media, I would use dimensional modeling with fact tables to capture user activities like posts, 
likes, comments, and shares, and dimension tables to describe users, content, time, and platform attributes.
The design focuses on analytics such as engagement, growth, and content performance.

                    Dim_User
          ┌────────────────────────┐
          │ user_id (PK)           │
          │ username               │
          │ age                    │
          │ gender                 │
          │ country                │
          │ signup_date            │
          └──────────┬─────────────┘
                     │
                     │
Dim_Time      ┌───────▼───────────┐       Dim_Post
┌──────────┐  │Fact_User_Activity │  ┌───────────────┐
│ time_id  │  │-------------------│  │ post_id (PK)  │
│ date     │  │ user_id (FK)      │  │ post_type     │
│ day      │◄─┤ post_id (FK)      ├─►│ category      │
│ month    │  │ time_id (FK)      │  │ created_date  │
│ year     │  │ device_id (FK)    │  └───────────────┘
└──────────┘  │                   │
              │ like_count        │
              │ comment_count     │
              │ share_count       │
              │ view_count        │
              └─────────┬─────────┘
                        │
                        │
                 Dim_Device
          ┌────────────────────────┐
          │ device_id (PK)         │
          │ device_type            │
          │ OS                     │
          │ app_version            │
          └────────────────────────┘
```

#### Q-6 how to explain upstream and downstream clearly and confidently ? 
```bash
In a data pipeline, upstream refers to the systems or processes that provide input data, while downstream refers to 
the systems or processes that consume the output data.
Any change or failure upstream can impact downstream processes.

Source Systems → Spark ETL → Data Warehouse → BI Dashboard
   (Upstream)                    (Downstream)

Upstream → Data producers
Downstream → Data consumers
Impact rule → Upstream issues propagate downstream

For example, in a Spark ETL pipeline, the source database or Kafka topic is upstream.
The Spark job itself is in the middle, and the data warehouse tables, dashboards, or reports that use the output 
are downstream.
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