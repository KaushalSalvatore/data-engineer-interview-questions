### ADF

#### Q-1 ? 
```bash
```

#### Q-2 How do you implement parameterization in Azure Data Factory pipelines ?
```bash
Simple example :-
Suppose you have 20 source tables: customer,account,transaction,loan,branch

Without parameterization, you might create:Customer_Pipeline,Account_Pipeline,Transaction_Pipeline,Loan_Pipeline

2. Pipeline parameters :- ADF → Pipeline → Parameters → New
ADF → Pipeline → Parameters → New

id="0h5n1a"
Parameter Name : source_table
Type           : String
Default Value  : customer

Then inside an activity you can reference it as: @pipeline().parameters.source_table

| Parameter                    | Variable                 |
| ---------------------------- | ------------------------ |
| Passed into pipeline         | Created inside pipeline  |
| Read-only during run         | Can be changed           |
| Used for configuration/input | Used for temporary state |
| Good for table/path/date     | Good for counters/flags  |
| `@pipeline().parameters.x`   | `@variables('x')`        |

I use parameterization to make my ADF pipelines reusable and avoid hardcoding table names, file paths, dates, 
and environment-specific values. I define pipeline-level parameters such as source table, source schema, file path, 
load date, load type and watermark column, and reference them using expressions like @pipeline().parameters.source_table.
```

#### Q-3 Describe how error handling and retry mechanisms work in ADF ?
```bash
ADF allows you to configure: Retry count , Retry interval , Timeout
Retry is mainly useful for transient failures : 
-> temporary network issue
-> database connection timeout
-> temporary service unavailable
-> throttling
-> intermittent API failure

                Copy Activity
                     │
              ┌──────┴──────┐
              │             │
          SUCCESS          FAILED
              │             │
              ↓             ↓
         Next Activity   Log Error
                            ↓
                      Send Notification
                            ↓
                       Alert Support
``` 

#### Q-4 ADF main components and work ? 
```bash
                Azure Data Factory
                       │
       ┌───────────────┼────────────────┐
       ↓               ↓                ↓
   Pipelines       Datasets         Linked Services
       │
       ↓
 Activities
       │
 ┌─────┼─────────────┐
 ↓     ↓             ↓
Copy  Data Flow   Stored Procedure
       │
       ↓
    ADLS / DB / DW
       │
       ↓
    Monitoring

1. Pipeline :- A pipeline is a logical container of activities. = Complete workflow 

Pipeline: Daily_Sales_Load
Copy Activity -> Data Validation -> Mapping Data Flow -> Stored Procedure -> Notification

2. Activity :- An activity is one task/operation inside a pipeline.
| Activity          | Purpose                    |
| ----------------- | -------------------------- |
| Copy Activity     | Move data                  |
| Mapping Data Flow | Transform data             |
| Lookup            | Read a value/configuration |
| Get Metadata      | Get file/table metadata    |
| ForEach           | Loop through items         |
| If Condition      | Conditional processing     |
| Stored Procedure  | Execute database procedure |
| Web Activity      | Call REST API              |
| Execute Pipeline  | Trigger another pipeline   |
| Delete Activity   | Delete files               |
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