#### Q-1 how to handle HDFS (Hadoop Distributed File System) fault tolerance ? 
```bash
All about making sure data is safe and accessible even when hardware fails (which will happen in distributed systems)

How HDFS Handles Fault Tolerance

1. Block Replication (Core Concept)
Files in HDFS are split into blocks (default ~128 MB).
Each block is replicated across multiple nodes (default replication factor = 3).

Block A → Node1, Node2, Node3

2. Rack Awareness
HDFS places replicas intelligently across different racks.

Typical placement:

1 replica → same node
2nd replica → different rack
3rd replica → same rack as 2nd

✔ Protects against:

Node failure ✅
Rack failure (power/network) ✅

3. Heartbeat Mechanism
Each DataNode sends heartbeat signals to NameNode (every ~3 seconds).

❌ If heartbeat stops:

NameNode marks node as dead
Triggers replication of missing blocks

4. Block Reports
DataNodes periodically send block metadata to NameNode.

✔ Helps NameNode:

Track which blocks exist
Detect missing or corrupt blocks

5. Automatic Re-Replication
When a node fails:
NameNode detects under-replicated blocks
New replicas are created on other healthy nodes

✔ Fully automatic — no manual intervention needed

🧠 Real-World Failure Scenarios
Scenario 1: DataNode crashes
✔ Handled by:
Replication
Re-replication

Scenario 2: Rack failure
✔ Handled by:
Rack-aware placement

Scenario 3: Corrupt data block
✔ Handled by:
Checksum + replica fetch

Scenario 4: NameNode failure
✔ Handled by:
HA (Active/Standby + ZooKeeper)
```

#### Q-2 I can draw a simple architecture diagram or give a real-world production example (Kafka → Spark → Snowflake).
```bash
        ┌────────────────────┐
        │   Data Sources     │
        │ (Apps, APIs, DBs)  │
        └─────────┬──────────┘
                  │
                  ▼
        ┌────────────────────┐
        │      Kafka         │
        │ (Topics/Partitions)│
        └─────────┬──────────┘
                  │
        (Streaming Consumption)
                  │
                  ▼
        ┌──────────────────────────┐
        │   Spark Structured       │
        │       Streaming          │
        │ - Transformations        │
        │ - Validation             │
        │ - Deduplication          │
        │ - Checkpointing          │
        └─────────┬────────────────┘
                  │
        ┌─────────┴──────────┐
        │                    │
        ▼                    ▼
┌───────────────┐    ┌────────────────┐
│   DLQ Kafka   │    │   Snowflake    │
│ (Bad Records) │    │ (Final Tables) │
└───────────────┘    └────────────────┘
```

#### Q-3 Normalization vs Denormalization ? 
```bash
```

#### Q-4 low offset and high offset in kafka  ? 
```bash
```

#### Q-5 Difference between processing time and event time in kafka ? 
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