#### Q-1 Docker Command for Run Kafka in Docker ? 
```bash
Docker-compose up or Docker-compose up -d
docker-compose down -v
docker ps

kafka container

docker exec -it kafka bash 
cd /opt/kafka/bin
ls (list of inside folder)
```

#### Q-2 ordering message in kafka ?
```bash
message ordering is guaranteed only within a partition, not across the entire topic.

Ordering is guaranteed only within a partition. Across multiple partitions Kafka does not guarantee 
global ordering.
```

#### Q-3 What is the best way to start the Kafka server ?
```bash
Once you download the latest version of Apache Kafka, remember to extract it.

To run Kafka, remember that your local environment must have Java 8+ installed on it.

If you want to start the Kafka server, the following commands have to be run in order so that 
all the services can be started in the correct order:

Start the ZooKeeper service:

$bin/zookeeper-server-start.sh config/zookeeper.properties

Open another terminal and run the following to start the Kafka broker service:

$ bin/kafka-server-start.sh config/server.properties
```

#### Q-4  What is the command to start ZooKeeper ?
```bash
  bin/zookeeper-server-start.sh
```

#### Q-5 Explain how topics can be added and removed ?
```bash
To create a topic:

kafka/bin/kafka-topics.sh --create \
--zookeeper localhost:2181 \
--replication-factor [replication factor] \
--partitions [number_of_partitions] \
--topic [unique-topic-name]

Delete Topic : 
Go to ${kafka_home}/config/server.properties, and add the below line:

Delete.topic.enable = true

Start the Kafka server once again with the new configuration:
${kafka_home}/bin/kafka-server-start.sh ~/kafka/config/server.properties

Delete the topic:
${kafka_home}/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic topic-name
```

#### Q-6 command for Create a topic ? 
```bash
docker exec -it kafka bash 
cd /opt/kafka/bin

./kafka-topics.sh --create \
--topic orders \
--partitions 3 \
--replication-factor 1 \
--bootstrap-server host.docker.internal:29092
```

#### Q-7 Check created topic name ?
```bash
./kafka-topics.sh \
--list \
--bootstrap-server host.docker.internal:29092
```

#### Q-8 Describe topic ?
```bash
./kafka-topics.sh \
--describe \
--topic orders \
--bootstrap-server host.docker.internal:29092
```

#### Q-9 Produce Test Data with help of producer ? 
```bash
./kafka-console-producer.sh \
--topic orders \
--bootstrap-server host.docker.internal:29092

Type a few messages manually:

order-1 
order-2 
order-3 
order-4
```

#### Q-10 Start a Consumer Group ? 
```bash
./kafka-console-consumer.sh \
--topic orders \
--group orders-consumer-group \
--from-beginning \
--bootstrap-server host.docker.internal:29092
```

#### Q-11 Inspect Consumer Group Lag ? 
```bash
./kafka-consumer-groups.sh \
--describe \
--group orders-consumer-group \
--bootstrap-server host.docker.internal:29092
```

#### Q-12 Create a producer pythobn code ?
```bash
Virtual Environment
python -m venv kafka-env
kafka-env\Scripts\activate

Install Kafka Client Library
pip install kafka-python

run producer.py 
```

#### Q-13 Consume from the beginning ? 
```bash
./kafka-console-consumer.sh \
--topic orders \
--from-beginning \
--bootstrap-server host.docker.internal:29092
```

#### Q-14 Start Console Producer with Key Support ? 
```bash
./kafka-console-producer.sh \
--topic orders \
--bootstrap-server host.docker.internal:29092 \
--property "parse.key=true" \
--property "key.separator=:"
```

#### Q-15 difference between Kafka with ZooKeeper and Kafka using KRaft (without ZooKeeper) ? 
```bash
old way 
Earlier versions of Apache Kafka depended on ZooKeeper to:
Manage brokers
Store metadata
Handle leader election
Maintain cluster state
Kafka could not run without ZooKeeper.

new way 
KRaft = Kafka Raft Metadata mode
Introduced to remove ZooKeeper dependency
Kafka now manages metadata internally
Uses the Raft consensus algorithm
Kafka runs without ZooKeeper

Why Kafka Removed ZooKeeper?

ZooKeeper was hard to manage
Extra operational overhead
Kafka wanted to be self-managed
KRaft improves:
Stability
Scalability
Simplicity
```

#### Q-16 what is Murmur2 Hashing ? 
```bash
Murmur2 is a fast, non-cryptographic hash function designed for hash-based lookups, not security.
It is very popular in Apache Kafka for partition assignment

partition = murmur2(key) % number_of_partitions
```

#### Q-17 What is the difference between At-most-once, At-least-once, and Exactly-once ?
```bash
At-most-once: Messages may be lost, no duplicates
At-least-once: No message loss, possible duplicates
Exactly-once: No loss, no duplicates (hardest, uses transactions)
```

#### Q-18 Detect Duplicate Messages (At-Least-Once) ?
```bash
essages = [1, 2, 3, 2, 4, 1]

duplicates = set([m for m in messages if messages.count(m) > 1])
print(duplicates)
```

#### Q-19 how to manage quality of data in Kafka when data is streaming if there is any issue in data then how to handle it without effecting actual pipeline ?
```bash
1. Real-time Validation in Consumers

In your processing layer (like Apache Spark or Kafka consumers):

Check nulls, data types
Apply business rules (e.g., amount > 0)

2. Dead Letter Queue (DLQ) — Most Important

Never break the pipeline because of bad data

Invalid records are sent to a separate Kafka topic (DLQ)
Main pipeline continues processing valid data
Main Topic → Processing → Valid → Target  
                      → Invalid → DLQ

✔️ Ensures:

No pipeline failure
Easy reprocessing later

4. Data Classification Strategy

Good data → continue pipeline
Bad data → DLQ
Suspicious data → quarantine topic

5. Idempotent Processing
Ensure reprocessing DLQ data doesn’t create duplicates
Use unique keys / upserts

Real-world Flow :-
Producer → Kafka Topic → Spark Consumer
                         ↓
                Validation Layer
                 ↓           ↓
            Valid Data     Invalid Data
               ↓               ↓
            Snowflake        DLQ Topic

✅ 1. Read from Kafka
spark = SparkSession.builder.appName("KafkaDLQExample").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input_topic") \
    .load()

✅ 2. Define Schema & Parse JSON
schema = StructType() \
    .add("id", StringType()) \
    .add("name", StringType()) \
    .add("amount", IntegerType())

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

✅ 3. Apply Validation Rules
valid_df = parsed_df.filter(
    (col("id").isNotNull()) &
    (col("amount") > 0)
)

invalid_df = parsed_df.subtract(valid_df)

✅ 4. Write Valid Data (Example: Console / DB / Snowflake)
valid_query = valid_df.writeStream \
    .format("console") \
    .option("checkpointLocation", "/tmp/check_valid") \
    .start()

✅ 5. Send Invalid Data to DLQ (Kafka Topic)
invalid_query = invalid_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "dlq_topic") \
    .option("checkpointLocation", "/tmp/check_dlq") \
    .start()

✅ 6. Start Streaming
spark.streams.awaitAnyTermination()

Key Points to Explain in Interview
DLQ topic (dlq_topic) stores bad records
Checkpointing ensures fault tolerance
Valid/Invalid split keeps pipeline running
Can reprocess DLQ later after fixing data
```

#### Q-20 I have data pipeline with Kafka and spark so how I design so I can handle consistency and data failures ? 
```bash
I would design the Kafka + Spark pipeline using exactly-once or at-least-once semantics, idempotent processing, 
checkpointing, and dead-letter handling to ensure consistency and handle failures.

1. Message Delivery Semantics
In Apache Kafka:
Enable idempotent producers
Use acks=all for durability

from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 1000000,
    'max.in.flight.requests.per.connection': 5
}

producer = Producer(conf)

producer.produce('my_topic', key='key1', value='message1')
producer.flush()

Idempotent producer = guarantees that messages are written exactly once to Kafka, even if retries happen.
Problem without idempotence:
Producer sends message
Network issue → no acknowledgment received
Producer retries
👉 Same message gets written twice (duplicate)

With idempotence enabled:
Kafka assigns:
Producer ID (PID)
Sequence number for each message
Broker checks:
If message with same sequence already exists → discard duplicate

acks=all ensures that a message is acknowledged only after all replicas have successfully stored it.

🔹 With acks=all:
Producer sends message
Leader writes it
Followers replicate it
Only then → acknowledgment sent

✔️ Result:
Message is durable even if a broker fails

2. Spark Structured Streaming with Checkpointing
In Apache Spark:
Enable checkpointing
Store offsets + state in durable storage (HDFS/S3)

df.writeStream \
  .option("checkpointLocation", "/path/checkpoint") \
  .start()

✔️ This ensures:

Recovery from failures
No data loss
No reprocessing beyond controlled limits

3. Idempotent Processing (Very Important)
Design transformations so reprocessing doesn’t create duplicates
Use:
Primary keys / deduplication logic
Merge/upsert instead of insert

4. Handle Data Failures (Bad Records)
Route invalid data to a Dead Letter Queue (DLQ) in Kafka
Keep pipeline running instead of failing completely

5. Exactly-Once Sink Writes
Use transactional writes or:
Upserts (MERGE) in target systems (like Snowflake)
Avoid duplicate inserts during retries

7. Backpressure & Scaling
Enable Spark backpressure
Scale consumers dynamically to handle spikes

How this ensures consistency:
Kafka guarantees durable event storage
Spark checkpointing ensures state recovery
Idempotency ensures no duplicates
DLQ ensures bad data doesn’t break pipeline
```