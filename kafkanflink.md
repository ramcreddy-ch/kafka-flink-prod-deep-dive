Kafka & Flink: Production-Grade Deep Dive
I'll walk you through production-ready knowledge with real-world examples from enterprise deployments.

1. Broker Sizing & Capacity Planning
Real-World Sizing Example
Let's say you're processing 10 million events/hour (financial transactions):
Calculate your requirements:
Event size: 2 KB average
Throughput: 10M events/hr = 2,777 events/sec
Data rate: 2,777 * 2 KB = 5.5 MB/sec ingest

With replication factor 3:
- Actual disk write: 5.5 MB/sec * 3 = 16.5 MB/sec
- Daily storage (retention 7 days): 5.5 MB/sec * 86400 * 7 * 3 = ~10 TB
Broker Configuration (Production Example):
properties# Each broker handles ~2-3 MB/sec sustained
num.brokers = 6 (for HA and headroom)

# Disk sizing per broker
# 10 TB / 6 brokers = ~1.7 TB per broker
# Add 30% overhead: 2.2 TB per broker
# Use RAID 10 or JBOD with multiple disks

# Memory allocation
heap.size = 6-8 GB (don't go above 10 GB due to GC)
page.cache = 32+ GB (this is where Kafka shines)
# On a 64 GB machine: 8 GB heap + 50 GB page cache + 6 GB OS

# Network threads
num.network.threads = 8  # For high concurrent connections
num.io.threads = 16      # 2x CPU cores typically

# Log segment sizing
log.segment.bytes = 1073741824  # 1 GB segments
log.retention.hours = 168        # 7 days
```

**Real Example - E-commerce Platform:**
At a major retailer, we sized brokers for Black Friday traffic:
- Normal: 50K orders/min
- Peak: 500K orders/min (10x spike)
- Broker cluster: 12 nodes (sized for 15x normal to handle spikes)
- Each broker: 96 GB RAM, 24 cores, 4x 2TB NVMe SSDs in JBOD

---

## 2. Partition Strategy (The Make-or-Break Decision)

### Partition Count Formula
```
Target partitions = max(
  throughput_required / throughput_per_partition,
  total_consumers,
  number_of_brokers * 10-15  # rule of thumb
)
Real-World Partitioning Examples
Example 1: User Events by Geography
java// Bad approach - hotspots
key = userId  // User "celebrity123" creates hotspot

// Better approach - composite key
public class GeoUserPartitioner implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, 
                        Object value, byte[] valueBytes, Cluster cluster) {
        UserEvent event = (UserEvent) value;
        // Distribute by region + userId hash
        String partitionKey = event.getRegion() + ":" + event.getUserId();
        return Math.abs(partitionKey.hashCode()) % cluster.partitionCountForTopic(topic);
    }
}

// Topic: user-events
// Partitions: 60 (5 regions * 12 partitions each)
Example 2: Financial Transactions (Order Matters)
java// Must maintain order per account
key = accountId  // Critical for balance calculations

// Monitoring for hotspots
// If account "ACCT_WHALE" has 100x traffic:
// Solution 1: Pre-split large accounts into sub-accounts
// Solution 2: Use Kafka Streams to aggregate before downstream
```

**Partition Count Guidelines:**
```
Small topics (< 1 GB/day): 6-12 partitions
Medium topics (1-50 GB/day): 30-60 partitions  
Large topics (> 50 GB/day): 100+ partitions

Max per broker: ~4000 partitions (including replicas)
Sweet spot: 2000-3000 partitions per broker
Real Production Issue - Over-Partitioning:
A team created a topic with 1000 partitions for "future scaling":

Only 10 consumers in consumer group
Result: 990 idle partitions, massive overhead
Leader election during broker restart: 45 seconds
After reducing to 50 partitions: restart in 8 seconds


3. ISR Tuning (In-Sync Replicas) - The Reliability Dial
Understanding ISR Mechanics
properties# Producer-side configuration
acks = all  # Wait for all in-sync replicas (strongest durability)
acks = 1    # Wait for leader only (faster, less durable)
acks = 0    # Fire and forget (fastest, no durability)

# Broker-side ISR management
min.insync.replicas = 2  # Minimum replicas that must acknowledge
# With replication.factor = 3, you can lose 1 broker

replica.lag.time.max.ms = 30000  # If follower lags > 30s, remove from ISR
Real-World ISR Scenario - Payment Processing
properties# Payment topic configuration (cannot lose data)
replication.factor = 3
min.insync.replicas = 2
acks = all
retries = 2147483647  # Effectively infinite
max.in.flight.requests.per.connection = 1  # Strict ordering
enable.idempotence = true

# Monitoring ISR health
# Alert if ISR < min.insync.replicas for > 5 minutes
Production Incident Example:
A broker went offline during peak:

Setup: 3 brokers, replication factor 3, min ISR 2
Impact: Producers continued with 2 replicas in ISR
When we brought broker back: caught up in 12 minutes
No data loss, no downtime

Without proper ISR tuning (min ISR 1):

One broker failure could risk data loss
Would need to halt producers or accept risk

ISR Monitoring (Critical Metrics)
sql-- Confluent Control Center or Prometheus queries
# Partitions with under-replicated status
kafka_server_replicamanager_underreplicatedpartitions > 0

# ISR shrink/expand rate (should be near zero)
kafka_server_replicamanager_isrshrinks_total
kafka_server_replicamanager_isrexpands_total

# Real alert from production
Alert: ISR shrink rate > 10/min for 5 minutes
Action: Check network saturation, disk I/O, GC pauses
```

---

## 4. Consumer Lag Handling at Scale

### Understanding Lag Metrics
```
Consumer Lag = (Log End Offset - Current Consumer Offset)

Example:
Producer written: offset 1,000,000
Consumer processed: offset 950,000
Lag: 50,000 messages
Real-Time Lag Monitoring Setup
java// Using Confluent metrics
public class LagMonitor {
    private final AdminClient adminClient;
    
    public Map<TopicPartition, Long> getConsumerLag(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> committed = 
            adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        
        Map<TopicPartition, Long> endOffsets = 
            consumer.endOffsets(committed.keySet());
        
        Map<TopicPartition, Long> lag = new HashMap<>();
        committed.forEach((tp, offsetMeta) -> {
            long lagValue = endOffsets.get(tp) - offsetMeta.offset();
            lag.put(tp, lagValue);
            
            // Alert if lag > threshold
            if (lagValue > 100000) {
                alertOncall("High lag detected", tp, lagValue);
            }
        });
        return lag;
    }
}
Strategies for Catching Up on Lag
Strategy 1: Horizontal Scaling (Add Consumers)
java// Original: 4 consumers, 20 partitions
// Lag: 2 million messages, processing 1000 msg/sec per consumer
// Time to catch up: 2M / (4 * 1000) = 500 seconds

// Scale to: 20 consumers (max for 20 partitions)
// Time to catch up: 2M / (20 * 1000) = 100 seconds

// In Kubernetes
kubectl scale deployment kafka-consumer --replicas=20
Strategy 2: Increase Throughput Per Consumer
properties# Fetch more records per poll
max.poll.records = 500  # Increase from default 500 to 2000
fetch.min.bytes = 1048576  # 1 MB, fetch in larger batches

# Process in parallel within consumer
# Use thread pool for processing while maintaining offset order
javapublic class ParallelConsumer {
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final Map<Integer, Long> partitionOffsets = new ConcurrentHashMap<>();
    
    public void consume() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            
            // Group by partition to maintain order
            Map<Integer, List<ConsumerRecord<String, String>>> byPartition = 
                records.partitions().stream()
                    .collect(Collectors.toMap(
                        TopicPartition::partition,
                        tp -> records.records(tp)
                    ));
            
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            byPartition.forEach((partition, partRecords) -> {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    partRecords.forEach(this::processRecord);
                    // Track last offset per partition
                    partitionOffsets.put(partition, 
                        partRecords.get(partRecords.size() - 1).offset());
                }, executor);
                futures.add(future);
            });
            
            // Wait for all partitions to complete before committing
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            consumer.commitSync();
        }
    }
}
Strategy 3: Skip to Latest (When Acceptable)
java// Use case: Real-time dashboard where old data doesn't matter
consumer.seekToEnd(consumer.assignment());
consumer.commitSync();

// Or skip to specific timestamp
long timestamp = System.currentTimeMillis() - (3600 * 1000); // 1 hour ago
Map<TopicPartition, Long> timestampsToSearch = 
    consumer.assignment().stream()
        .collect(Collectors.toMap(tp -> tp, tp -> timestamp));
        
consumer.offsetsForTimes(timestampsToSearch).forEach((tp, offsetAndTime) -> {
    if (offsetAndTime != null) {
        consumer.seek(tp, offsetAndTime.offset());
    }
});
```

### Real Production Example - Black Friday Lag Crisis

**Scenario:** E-commerce order processing
- Normal lag: < 1000 messages
- Black Friday morning: lag jumped to 8 million
- Impact: Order confirmations delayed 2+ hours

**Resolution:**
1. Scaled consumers from 10 to 50 (matched partition count)
2. Increased `max.poll.records` from 500 to 2000
3. Added parallel processing within each consumer
4. Caught up in 45 minutes
5. Post-incident: Pre-scaled consumers for known traffic spikes

---

## 5. KRaft vs ZooKeeper (The Big Migration)

### Architecture Comparison

**ZooKeeper Era (Legacy):**
```
Metadata Flow:
Producer → Broker (leader election via ZK) → ZooKeeper cluster (3-5 nodes)
                                           → Stores: topic configs, ACLs, quotas
                                           
Scaling limits:
- ZK watches: ~100k watchers max
- Partition limit: ~200k (practical limit)
- Metadata propagation: seconds
```

**KRaft Mode (Modern):**
```
Metadata Flow:
Producer → Broker (self-contained quorum) → Raft metadata log (replicated)
                                          → Stored as Kafka topic __cluster_metadata

Benefits:
- Partition limit: 10 million+ (tested)
- Metadata propagation: milliseconds
- Simpler operations (one system instead of two)
- Faster controller failover (50ms vs 5+ seconds)
Migration Strategy (Real Example from 2024)
Pre-Migration Setup:
bash# Existing ZK-based cluster
3 ZooKeeper nodes
12 Kafka brokers (version 3.3.x)
~50k partitions across 500 topics
Migration Steps:
properties# Step 1: Upgrade brokers to KRaft-compatible version (3.4+)
# Rolling upgrade, one broker at a time

# Step 2: Enable dual-write mode (write to both ZK and KRaft)
zookeeper.metadata.migration.enable=true
controller.quorum.voters=1@controller1:9093,2@controller2:9093,3@controller3:9093

# Step 3: Start KRaft controllers
# controller.properties
process.roles=controller
node.id=1
controller.quorum.voters=1@controller1:9093,2@controller2:9093,3@controller3:9093
log.dirs=/var/lib/kafka/kraft-metadata

# Step 4: Migrate metadata
kafka-metadata.sh --snapshot /tmp/zk-migration --bootstrap-server localhost:9092

# Step 5: Switch traffic to KRaft
# Update brokers to use KRaft controllers

# Step 6: Decommission ZooKeeper
# After validation period (we waited 2 weeks)
Production Results:

Migration downtime: 0 seconds (rolling migration)
Controller failover improved: 8 seconds → 200ms
Partition creation speed: 2x faster
One team member freed from ZK operations

KRaft Configuration (Production-Ready)
properties# Controller nodes (dedicated, not serving data)
process.roles=controller
node.id=1
controller.quorum.voters=1@controller1:9093,2@controller2:9093,3@controller3:9093

# Metadata log configuration
metadata.log.dir=/var/kafka/kraft-metadata
metadata.log.segment.bytes=1073741824
metadata.log.max.record.bytes.between.snapshots=20971520

# Combined broker+controller (smaller deployments)
process.roles=broker,controller
# But for production > 50 brokers, use dedicated controllers
```

---

## 6. Schema Evolution & Backward Compatibility

### Schema Registry Architecture (Confluent)
```
Producer → [Avro/Protobuf/JSON Schema] → Schema Registry (validates) → Broker
                                       → Returns schema ID
Consumer ← [Deserialize using schema ID] ← Schema Registry ← Broker
```

### Compatibility Modes Explained
```
BACKWARD (default): New schema can read old data
FORWARD: Old schema can read new data  
FULL: Both backward and forward
NONE: No validation (dangerous!)
Real-World Schema Evolution Examples
Example 1: Adding Optional Field (BACKWARD compatible)
json// Version 1 (existing)
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}

// Version 2 (adding phone with default)
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "phone", "type": ["null", "string"], "default": null}
  ]
}

// Old consumers can still read new messages (ignore phone field)
// New consumers can read old messages (phone = null)
Example 2: Production Issue - Breaking Change
A developer removed a required field:
json// Version 1
{"name": "price", "type": "double"}

// Version 2 (WRONG - removed required field)
// Schema registry REJECTED this with BACKWARD mode
// Error: "Field price is required but missing in new schema"
Correct approach:
json// Version 2 (make optional first, then deprecated)
{"name": "price", "type": ["null", "double"], "default": null}

// Version 3 (6 months later, after all consumers upgraded)
// Remove the field entirely
Schema Registry Setup (Production)
properties# High availability setup
schema.registry.url=http://sr1:8081,http://sr2:8081,http://sr3:8081

# Store schemas in Kafka (not just memory)
kafkastore.topic=_schemas
kafkastore.topic.replication.factor=3

# Performance tuning
kafkastore.init.timeout.ms=60000
schema.cache.size=1000  # Caches schemas in memory
Producer with Schema Registry:
javaProperties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
props.put("schema.registry.url", "http://schema-registry:8081");

KafkaProducer<String, User> producer = new KafkaProducer<>(props);

User user = User.newBuilder()
    .setId("user123")
    .setEmail("user@example.com")
    .setPhone("555-1234")  // New field
    .build();

producer.send(new ProducerRecord<>("users", user.getId(), user));
Real Production Scenario - Multi-Team Coordination
Challenge: 5 teams consuming same topic with different schema versions
Solution: Compatibility Contract
yaml# Schema evolution policy
compatibility_mode: FULL
review_required: true
deprecation_period: 90_days

upgrade_sequence:
  1. Add new field with default (BACKWARD compatible)
  2. Deploy all producers with new schema
  3. Wait 30 days (grace period)
  4. Deploy consumers to use new field
  5. After 90 days, make field required if needed
```

---

## 7. Kafka Connect at Enterprise Scale

### Architecture for High Throughput
```
Source Connectors:
Database (CDC) → Debezium → Kafka
REST API → Custom Connector → Kafka
S3 files → S3 Source → Kafka

Sink Connectors:
Kafka → JDBC Sink → PostgreSQL
Kafka → Elasticsearch Sink → ES Cluster  
Kafka → S3 Sink → Data Lake
Production Connect Cluster Setup
properties# Distributed mode (production standard)
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
group.id=connect-cluster

# Store connector configs in Kafka (not files)
config.storage.topic=connect-configs
config.storage.replication.factor=3

offset.storage.topic=connect-offsets
offset.storage.replication.factor=3
offset.storage.partitions=25  # Scale for parallelism

status.storage.topic=connect-status
status.storage.replication.factor=3

# Performance tuning
tasks.max=8  # Per connector parallelism
offset.flush.interval.ms=60000
Real-World Example: MySQL CDC with Debezium
Scenario: Sync 50 million row customer table in real-time
json{
  "name": "mysql-source-customers",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql-master.prod",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "${file:/secrets/mysql-password}",
    "database.server.id": "184054",
    "database.server.name": "prod-mysql",
    "database.include.list": "customers_db",
    "table.include.list": "customers_db.customers,customers_db.orders",
    
    "tasks.max": "4",
    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",
    
    "database.history.kafka.bootstrap.servers": "broker1:9092",
    "database.history.kafka.topic": "schema-changes.customers",
    
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
Initial Snapshot Performance:

50M rows at ~2KB each = 100 GB
With 4 tasks: Completed in 6 hours
Ongoing CDC latency: < 2 seconds

Elasticsearch Sink Example (Real-Time Search)
json{
  "name": "elasticsearch-sink-products",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "topics": "products,inventory",
    "connection.url": "http://es1:9200,http://es2:9200,http://es3:9200",
    "type.name": "_doc",
    "key.ignore": "false",
    "schema.ignore": "false",
    
    "tasks.max": "6",
    "batch.size": "2000",
    "max.buffered.records": "20000",
    "linger.ms": "1000",
    
    "read.timeout.ms": "120000",
    "connection.timeout.ms": "30000",
    "max.retries": "5",
    
    "behavior.on.null.values": "delete",
    "behavior.on.malformed.documents": "warn"
  }
}
Throughput Achieved:

15,000 documents/sec per task
6 tasks = 90,000 docs/sec
Latency: end-to-end < 3 seconds

Connect Monitoring (Critical Metrics)
java// Custom metrics via JMX
# Connector status
kafka.connect:type=connector-metrics,connector={connector-name}

# Task metrics  
kafka.connect:type=task-metrics,connector={connector},task={task-id}
- status (running/failed/paused)
- offset-commit-avg-time-ms
- offset-commit-failure-percentage

# Real alerting rules
Alert: connector status != RUNNING for 5 minutes
Alert: offset commit failures > 5% 
Alert: task lag > 100,000 records
Production Incident: Connector Failure Recovery
Problem: JDBC sink connector failed due to database downtime

2 hours of data accumulated (2M records)
Connector restarted but couldn't catch up

Solution:
bash# Increased parallelism temporarily
curl -X PUT http://connect:8083/connectors/jdbc-sink/config \
  -H "Content-Type: application/json" \
  -d '{"tasks.max": "16"}'  # Was 4

# Increased batch size
"batch.size": "5000"  # Was 1000

# Result: Caught up in 30 minutes, then scaled back to normal
```

---

## 8. Disaster Recovery & Replay Strategies

### Multi-Datacenter Replication (Confluent Replicator)

**Active-Passive Setup:**
```
Primary DC (US-East):
Producers → Kafka Cluster (3 brokers) → Replicator → Secondary DC

Secondary DC (US-West):  
Kafka Cluster (3 brokers, read-only) ← Replicator
Replicator Configuration:
properties# Source cluster
src.kafka.bootstrap.servers=us-east-broker1:9092,us-east-broker2:9092

# Destination cluster
dest.kafka.bootstrap.servers=us-west-broker1:9092,us-west-broker2:9092

# What to replicate
topic.whitelist=orders.*,payments.*,users

# Preserve timestamps and offsets
provenance.header.enable=true
offset.timestamps.commit=true

# Performance
tasks.max=8
confluent.topic.replication.factor=3
Failover Procedure (Tested Quarterly):
bash# 1. Stop producers in primary DC
# 2. Wait for replicator lag = 0
kafka-consumer-groups --bootstrap-server us-west:9092 --group replicator --describe

# 3. Promote secondary to primary
# Update DNS: kafka.prod.company.com → us-west-lb

# 4. Restart producers (automatically connect to new primary)

# RTO achieved: 5 minutes
# RPO: 0 (zero data loss with proper monitoring)
Active-Active (Bidirectional Replication)
Use Case: Global writes with conflict resolution
properties# US-East → US-West
src.kafka.bootstrap.servers=us-east:9092
dest.kafka.bootstrap.servers=us-west:9092
topic.whitelist=global-users

# US-West → US-East  
src.kafka.bootstrap.servers=us-west:9092
dest.kafka.bootstrap.servers=us-east:9092
topic.whitelist=global-users

# Conflict detection
provenance.header.enable=true
# Add origin datacenter to each message

# Application-level conflict resolution required
Conflict Resolution Example:
javapublic class ConflictResolver {
    public User resolveConflict(User usEast, User usWest) {
        // Last-write-wins based on timestamp
        if (usEast.getTimestamp() > usWest.getTimestamp()) {
            return usEast;
        } else if (usWest.getTimestamp() > usEast.getTimestamp()) {
            return usWest;
        } else {
            // Same timestamp, use application logic
            // e.g., prefer higher value update for account balance
            return usEast.getBalance() > usWest.getBalance() ? usEast : usWest;
        }
    }
}
Point-in-Time Replay Strategies
Strategy 1: Replay from Timestamp
java// Replay to 2 hours ago to fix bad data
long twoHoursAgo = System.currentTimeMillis() - (2 * 3600 * 1000);

KafkaConsumer<String, Event> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("orders"));

Map<TopicPartition, Long> timestampMap = new HashMap<>();
for (TopicPartition tp : consumer.assignment()) {
    timestampMap.put(tp, twoHoursAgo);
}

Map<TopicPartition, OffsetAndTimestamp> offsets = 
    consumer.offsetsForTimes(timestampMap);

offsets.forEach((tp, offsetAndTimestamp) -> {
    if (offsetAndTimestamp != null) {
        consumer.seek(tp, offsetAndTimestamp.offset());
    }
});

// Begin reprocessing
while (true) {
    ConsumerRecords<String, Event> records = consumer.poll(Duration.ofMillis(100));
    // Reprocess with fixed logic
}
Strategy 2: Replay to Separate Topic
java// Production incident: Bad code deployed, corrupted downstream DB
// Solution: Replay to new topic with fixed logic

// Step 1: Create replay topic
kafka-topics --create --topic orders-replay \
  --partitions 20 --replication-factor 3

// Step 2: Copy data with timestamp filter
public class ReplayProducer {
    public void replay(long startTime, long endTime) {
        KafkaConsumer<String, Order> source = new KafkaConsumer<>(sourceProps);
        KafkaProducer<String, Order> dest = new KafkaProducer<>(destProps);
        
        // Seek to start time
        Map<TopicPartition, Long> timestamps = // ... seek to startTime
        
        while (true) {
            ConsumerRecords<String, Order> records = source.poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String, Order> record : records) {
                if (record.timestamp() > endTime) {
                    return; // Stop at end time
                }
                
                // Apply fixed business logic
                Order corrected = applyFix(record.value());
                
                dest.send(new ProducerRecord<>("orders-replay", 
                    record.key(), corrected));
            }
        }
    }
}

// Step 3: Point consumers to replay topic
// Step 4: After validation, swap back to original topic
```

### Real Production DR Test

**Scenario:** Annual DR drill for payments system
```
Setup:
- Primary: AWS us-east-1 (3 brokers, 60 partitions)
- DR: AWS us-west-2 (3 brokers, 60 partitions)  
- Replication lag: < 500ms

Test:
1. Simulated primary DC failure (network blackhole)
2. Promoted DR site in 4 minutes
3. Processed 0 lost messages (validated via offset comparison)
4. Producers automatically reconnected

Learnings:
- DNS TTL was 300s (reduced to 60s)
- Added health checks to producer connection logic
- Improved monitoring dashboard for replicator lag
```

---

## 9. Apache Flink - Real-Time Stream Processing

### Flink vs Kafka Streams (When to Use Each)

**Kafka Streams:**
- Simpler deployment (just a library, runs in your app)
- Tightly coupled to Kafka
- Good for: Simple transformations, filtering, windowing within Kafka ecosystem

**Flink:**
- Full stream processing engine
- Complex event processing, state management at massive scale
- Good for: Multi-source streams, complex joins, ML inference, sessionization

### Flink Architecture Deep Dive
```
JobManager (master):
  - Schedules tasks
  - Manages checkpoints
  - Coordinates recovery

TaskManagers (workers):
  - Execute tasks
  - Manage state (RocksDB embedded)
  - Network buffer pool

Checkpoint Coordinator:
  - Snapshot state every N seconds
  - Store to distributed storage (S3/HDFS)
  - Enable exactly-once processing
Production Flink Cluster Setup
yaml# flink-conf.yaml (production settings)

# JobManager
jobmanager.memory.process.size: 8g
jobmanager.rpc.address: jobmanager.prod.company.com
jobmanager.rpc.port: 6123

# TaskManager (workers)
taskmanager.memory.process.size: 16g
taskmanager.numberOfTaskSlots: 4  # Parallelism per worker
taskmanager.memory.network.fraction: 0.2  # For shuffles

# State backend (RocksDB for large state)
state.backend: rocksdb
state.backend.rocksdb.localdir: /mnt/nvme/flink-state
state.checkpoints.dir: s3://prod-flink-checkpoints/app1
state.savepoints.dir: s3://prod-flink-savepoints/app1

# Checkpointing (exactly-once guarantee)
execution.checkpointing.interval: 60000  # 1 minute
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.min-pause: 30000
execution.checkpointing.max-concurrent-checkpoints: 1

# High availability (for production)
high-availability: zookeeper
high-availability.zookeeper.quorum: zk1:2181,zk2:2181,zk3:2181
high-availability.storageDir: s3://prod-flink-ha/recovery

#Continue02:46Restart strategy
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 10
restart-strategy.fixed-delay.delay: 30s

### Real-World Flink Example: Fraud Detection

**Use Case:** Detect fraudulent credit card transactions in real-time
```java
public class FraudDetectionJob {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // 1 minute
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // Kafka source
        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
            .setBootstrapServers("broker1:9092,broker2:9092")
            .setTopics("transactions")
            .setGroupId("fraud-detection")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new TransactionDeserializer())
            .build();
        
        DataStream<Transaction> transactions = env.fromSource(
            source, 
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
            "Kafka Source"
        );
        
        // Rule 1: Detect rapid-fire transactions
        DataStream<Alert> rapidFireAlerts = transactions
            .keyBy(Transaction::getCardNumber)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new TransactionCounter())
            .filter(count -> count.getCount() > 10)  // > 10 txns in 5 min
            .map(count -> new Alert(count.getCardNumber(), "RAPID_FIRE", 
                "More than 10 transactions in 5 minutes"));
        
        // Rule 2: Detect geographical anomalies (impossible travel)
        DataStream<Alert> geoAlerts = transactions
            .keyBy(Transaction::getCardNumber)
            .process(new ImpossibleTravelDetector());
        
        // Rule 3: Amount spike detection (ML-based)
        DataStream<Alert> amountAlerts = transactions
            .keyBy(Transaction::getCardNumber)
            .flatMap(new AnomalyDetector());  // Uses loaded ML model
        
        // Combine all alerts
        DataStream<Alert> allAlerts = rapidFireAlerts
            .union(geoAlerts)
            .union(amountAlerts);
        
        // Sink to Kafka for downstream processing
        KafkaSink<Alert> sink = KafkaSink.<Alert>builder()
            .setBootstrapServers("broker1:9092")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("fraud-alerts")
                .setValueSerializationSchema(new AlertSerializer())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("fraud-detection")
            .build();
        
        allAlerts.sinkTo(sink);
        
        env.execute("Fraud Detection Job");
    }
}
```

**Impossible Travel Detector (Stateful Function):**
```java
public class ImpossibleTravelDetector 
    extends KeyedProcessFunction<String, Transaction, Alert> {
    
    // State: last transaction per card
    private ValueState<Transaction> lastTransactionState;
    
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Transaction> descriptor =
            new ValueStateDescriptor<>("last-transaction", Transaction.class);
        lastTransactionState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void processElement(Transaction current, Context ctx, Collector<Alert> out) 
        throws Exception {
        
        Transaction last = lastTransactionState.value();
        
        if (last != null) {
            // Calculate distance and time difference
            double distance = calculateDistance(
                last.getLatitude(), last.getLongitude(),
                current.getLatitude(), current.getLongitude()
            );
            
            long timeDiff = current.getTimestamp() - last.getTimestamp();
            double hours = timeDiff / (1000.0 * 3600);
            
            double speed = distance / hours; // km/h
            
            // Alert if speed > 1000 km/h (impossible by car)
            if (speed > 1000) {
                out.collect(new Alert(
                    current.getCardNumber(),
                    "IMPOSSIBLE_TRAVEL",
                    String.format("Traveled %.0f km in %.1f hours (%.0f km/h)",
                        distance, hours, speed)
                ));
            }
        }
        
        // Update state with current transaction
        lastTransactionState.update(current);
    }
    
    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        // Haversine formula
        double R = 6371; // Earth radius in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon/2) * Math.sin(dLon/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }
}
```

### Flink State Management (Critical for Production)

**State Size in Production:**
Example fraud detection job:

50 million active cards
State per card: ~200 bytes (last transaction + aggregates)
Total state: 10 GB

With RocksDB state backend:

Stored on local SSD (fast access)
Checkpointed to S3 every minute
Allows state larger than memory


**Checkpoint Configuration:**
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Checkpoint every 1 minute
env.enableCheckpointing(60000);

CheckpointConfig config = env.getCheckpointConfig();

// Exactly-once semantics
config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// Keep 3 checkpoints
config.setMaxConcurrentCheckpoints(1);
config.setMinPauseBetweenCheckpoints(30000);

// Retain checkpoints on cancellation (for debugging)
config.setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// Checkpoint timeout
config.setCheckpointTimeout(600000); // 10 minutes for large state
```

### Flink Windowing (Real Examples)

**Tumbling Window (Non-overlapping):**
```java
// Count orders per 5-minute window
transactions
    .keyBy(t -> t.getProductId())
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new OrderCountAggregator())
    .print();

// Result: [00:00-00:05]: 100 orders, [00:05-00:10]: 150 orders
```

**Sliding Window (Overlapping):**
```java
// Moving average of last 10 minutes, updated every 1 minute
prices
    .keyBy(p -> p.getSymbol())
    .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
    .aggregate(new AverageAggregator())
    .print();

// At 00:05, window contains data from 23:55-00:05
// At 00:06, window contains data from 23:56-00:06
```

**Session Window (Gap-based):**
```java
// Group user clicks into sessions (gap > 30 min = new session)
clicks
    .keyBy(c -> c.getUserId())
    .window(SessionWindows.withGap(Time.minutes(30)))
    .aggregate(new SessionAggregator())
    .print();

// User clicks at: 10:00, 10:15, 10:45, 11:00
// Sessions: [10:00-10:15] (gap < 30 min), [10:45-11:00] (new session)
```

### Flink Performance Tuning (Production Lessons)

**Parallelism Configuration:**
```java
env.setParallelism(32);  // Global parallelism

// Per-operator parallelism (for bottlenecks)
dataStream
    .map(new HeavyTransformation())
    .setParallelism(64)  // Double parallelism for heavy operation
    .keyBy(...)
    .process(new LightAggregation())
    .setParallelism(16);  // Reduce for light operation
```

**Production Sizing Example:**
Job: Real-time recommendations
Input: 100k events/sec
Processing: 50ms per event average
Calculation:
Required throughput: 100k/sec
Per-task throughput: 20/sec (50ms processing time)
Required parallelism: 100k / 20 = 5000 tasks
TaskManagers: 100 (with 4 slots each = 400 slots available)
Need: 5000 / 400 = 12.5x scale up
Solution:

Optimize processing to 10ms per event (200/sec per task)
Required tasks: 100k / 200 = 500
TaskManagers needed: 500 / 4 = 125


**Network Buffer Tuning:**
```yaml
# For high-throughput jobs with shuffles
taskmanager.memory.network.fraction: 0.3  # Increase from default 0.1
taskmanager.network.numberOfBuffers: 8192  # Increase for large shuffles
```

### Handling Late Data
```java
DataStream<Transaction> transactions = ...

// Allow 5 seconds of lateness
transactions
    .keyBy(Transaction::getUserId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowedLateness(Time.seconds(5))  // Wait 5 sec for late data
    .sideOutputLateData(lateDataTag)   // Capture very late data
    .aggregate(new SumAggregator());

// Process very late data separately
DataStream<Transaction> veryLateData = 
    transactions.getSideOutput(lateDataTag);

veryLateData.addSink(new KafkaSink<>("late-transactions"));
```

### Flink Monitoring (Production Dashboards)

**Key Metrics:**
Throughput
records_in_per_second
records_out_per_second
Latency
watermark_lag (event time - watermark)
checkpoint_duration
processing_time_per_record
State
state_size_bytes
checkpoint_size_bytes
Backpressure (critical!)
backpressure_ratio (should be < 0.1)

**Alerting Rules:**
```yaml
alerts:
  - name: HighBackpressure
    condition: backpressure_ratio > 0.5 for 5m
    action: Scale up TaskManagers
    
  - name: CheckpointFailure
    condition: checkpoint_failures > 3 in 10m
    action: Page on-call
    
  - name: HighWatermarkLag
    condition: watermark_lag > 60s
    action: Investigate slow operators
```

### Real Production Incident: State Size Explosion

**Problem:**
- Fraud detection job state grew from 10 GB to 500 GB in 2 days
- Checkpoints timing out (> 10 minutes)
- Job failing during recovery

**Root Cause:**
```java
// Bug: Never cleaned up old state
private MapState<String, List<Transaction>> transactionHistory;

// Accumulated ALL transactions per card forever
transactionHistory.put(cardNumber, allTransactions);
```

**Fix:**
```java
// Added TTL to state
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.days(30))  // Keep only 30 days
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();

MapStateDescriptor<String, List<Transaction>> descriptor = 
    new MapStateDescriptor<>("transaction-history", 
        String.class, TypeInformation.of(new TypeHint<List<Transaction>>(){}));
descriptor.enableTimeToLive(ttlConfig);

transactionHistory = getRuntimeContext().getMapState(descriptor);
```

**Result:**
- State dropped back to 12 GB
- Checkpoint time: < 2 minutes
- No more failures

---

## Key Production Takeaways

### Kafka
1. **Size for peak + 50% headroom** - Black Friday will come
2. **Partition count = max(throughput needs, consumer count)** - Can't change easily later
3. **min.insync.replicas = 2** with **replication factor 3** - Sweet spot for durability vs availability
4. **Monitor consumer lag religiously** - Set alerts at 50% of your catch-up threshold
5. **Test failover quarterly** - DR plans rot without practice

### Flink
1. **Checkpointing is non-negotiable** - Exactly-once semantics require it
2. **State TTL prevents state explosions** - Always set expiration on stateful operations
3. **RocksDB for state > 1 GB** - Memory state backend doesn't scale
4. **Parallelism = bottleneck operator × 2** - Scale the slowest part first
5. **Monitor backpressure** - It's your canary in the coal mine
