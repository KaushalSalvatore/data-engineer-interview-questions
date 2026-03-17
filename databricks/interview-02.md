#### Q-1 In the Medallion Architecture (widely used in Databricks), data flows through three layers: Bronze → Silver → Gold. Each layer has a clear responsibility ? 
```bash
1. Bronze Layer (Raw Data)

What it does:
Stores raw, unprocessed data
Exact copy of source data

-> Characteristics:
No cleaning
No transformations
Append-only (keep history)

-> Example:
Logs from apps
CSV/JSON files from APIs
Database dumps

-> Stored using:
Delta Lake

Purpose: -> “Keep the original truth”

2. Silver Layer (Cleaned Data)
🔹 What it does:
Cleans and standardizes data

🔹 Operations:
Remove duplicates
Handle missing values
Fix data types
Join datasets

🔹 Example:
Clean customer table
Validated transactions

👉 Processed using: Apache Spark

Purpose:“Make data reliable and usable”

3. Gold Layer (Business Data)
🔹 What it does:
Creates business-ready datasets

🔹 Operations:
Aggregations
KPIs
Metrics
Reporting tables

🔹 Example:
Total sales per month
Revenue by region
Customer lifetime value

🧠 Purpose: “Make data useful for decision-making”
```

#### Q-2
```bash
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