#### Q-1 Docker Command for Run Kafka in Docker ? 
```bash
Docker-compose up or Docker-compose up -d
docker-compose down -v
docker ps
```

#### Q-2 go to kafka container ?
```bash
docker exec -it kafka bash 
cd /opt/kafka/bin

ls (list of inside folder)
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

#### Q-19 What is Round-Robin Partitioning in Kafka ?
```bash
When a producer sends messages without a key, Kafka assigns each message to the next partition in sequence, 
looping back to the first partition after the last one.

Topic with 3 partitions:
P0, P1, P2

Messages without key:
M1 → P0
M2 → P1
M3 → P2
M4 → P0
M5 → P1
```

#### Q-20 when not to use kafka ? 
```bash
Simple point-to-point messaging
Small-scale applications
Request-response systems
Systems needing strict global ordering
```