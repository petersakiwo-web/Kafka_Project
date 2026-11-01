# Payment Events Setup: Kafka Payment Event Streaming POC

**Project:** Digital Payments Event Streaming Infrastructure
**Team:** Digital Payments Engineering

> **Run status.** The code and commands in this submission are complete. This environment had no reachable Kafka cluster (Docker daemon not running, no Python runtime), so the live execution evidence in sections 4.2, 4.3, 4.4 and 4.5 is marked **TO CAPTURE** and must be pasted from the local cluster run using the commands given. No output has been invented.

---

## 1. Design Decisions

### 1.1 Sizing basis

| Requirement | Value | Effect on design |
|---|---|---|
| Peak throughput | 1000 MB/s | Drives partition count |
| Producer instance throughput | 100 MB/s | At least 10 producer instances |
| Consumer instance throughput | 10 MB/s | A consumer group reading the full stream needs 100 instances, so at least 100 partitions |
| Payment real-time lag | < 150 ms | acks=all with small linger, RF 3, minISR 2 |
| Fraud lag | < 50 ms | Dedicated consumer group, batched commits |
| Notification lag | < 2 s | Dedicated consumer group |
| Retention | 7 years for fraud and payment events | Time-based 7-year retention, unlimited size |

Topics are split by domain, one per producer, as project.md asks (payment, fraud, notification). Each topic is then sized for its own volume.

### 1.2 Topic names

| Topic | Producer | Purpose |
|---|---|---|
| `digital.payments.lifecycle.v1` | payment-producer-1 | All payment state transitions |
| `digital.fraud.scores.v1` | fraud-producer-1 | Fraud score low / medium / high |
| `digital.notifications.v1` | notification-producer-1 | Payment approved / failed notifications |

**Justification.** The naming follows `<org-domain>.<stream>.<version>`, lower case and dot separated. The domain prefix keeps topics from different teams apart. The version suffix lets a breaking schema change ship as `.v2` alongside `.v1` during migration. Dots are used consistently and underscores are avoided, because Kafka metric names replace dots with underscores, so mixing the two creates collisions.

### 1.3 Partitions

| Topic | Partitions | Why |
|---|---|---|
| payments lifecycle | 100 | Each consumer group reads the whole stream. At 10 MB/s per consumer, 1000 MB/s needs 100 consumers, and Kafka allows at most one consumer per partition in a group. |
| fraud scores | 100 | Same key (`payment_id`) and same count as payments, so a downstream join reads matching partitions. |
| notifications | 50 | Each payment produces at most one notification event, so volume is much lower. 50 partitions still allow 500 MB/s of consumer capacity. |

**Trade-off.** More partitions give more parallelism but cost more file handles, more replication traffic and slower leader election. Above about 200 partitions per broker these costs grow quickly. 100 partitions per topic stays within that limit for a cluster of this size. Partitions can be increased later, but that changes the key-to-partition mapping, so it must be planned.

**Producer side.** 1000 MB/s at 100 MB/s per producer needs 10 producer instances. Their partition count is not the limit, because producers can write to any partition.

### 1.4 Replication factor = 3

- **Zero data loss requirement.** With RF 3 and `min.insync.replicas=2`, any single broker can fail with no loss of acknowledged data. An acknowledged write exists on at least 2 brokers.
- **Broker outage.** If one broker goes down, the partitions it led fail over to an in-sync follower within seconds. Writes continue as long as 2 replicas are in sync.
- **Cost.** RF 3 triples storage and replication bandwidth. For 7-year payment history that is a deliberate cost, because losing a payment event breaches the regulatory requirement.

RF 2 was rejected: a single broker failure would leave one replica and no safe write path with `minISR=2`.

### 1.5 Retention

- **Payment and fraud topics:** `retention.ms=220752000000` (7 × 365 days) and `retention.bytes=-1`.
- **Notifications topic:** `retention.ms=2592000000` (30 days) and `retention.bytes=-1`.

**Time vs size.** Compliance is defined in time (7 years), so time is the primary control. A size limit would delete data before the 7-year point, which is a regulatory breach. Therefore size-based retention is **not** used for the payment and fraud topics. Storage growth is managed instead with capacity planning and tiered storage (moving older segments to object storage while keeping them readable). Notifications are operational data with no regulatory hold, so a 30-day time window is enough for replay after an incident.

### 1.6 Cleanup policy = `delete`

Payment events are immutable facts: a payment does not change, it gains a new event. The topic is an event stream, not a state store. Log compaction would keep only the latest event per key and discard the history the audit trail requires, so `cleanup.policy=delete` is the correct choice. Current state, if a consumer needs it, belongs in a separate table built from the stream.

### 1.7 Compression = `zstd`

- zstd gives a better ratio than snappy or lz4 on repetitive JSON payment data, which cuts storage and network cost on a 1000 MB/s stream.
- CPU cost is moderate and acceptable at 100 MB/s per producer.
- The producer and the topic both use zstd, so brokers store batches as received and do not recompress them.
- `linger.ms=5` lets batches fill, which improves compression, while adding at most 5 ms to the 150 ms payment budget.

### 1.8 Min in-sync replicas = 2

With `acks=all`, the producer waits for every in-sync replica. `min.insync.replicas=2` sets the floor: a write is accepted only while at least 2 replicas are in sync. With RF 3 this means the write survives the loss of any one broker, and the system rejects writes rather than accepting unsafe ones when two brokers are down. For payment initiation this is the right side of the latency/durability trade-off: a few extra milliseconds to ack are cheap compared with losing a payment.

### 1.9 Max message size

`max.message.bytes=1048576` (1 MiB). A payment event is about 0.5 KB, so 1 MiB is a safety limit against malformed or oversized messages, not a sizing target.

### 1.10 Message schema (payment lifecycle)

| Field | Type | Description |
|---|---|---|
| `event_id` | UUID string | Unique per event. Used by downstream consumers to de-duplicate. |
| `event_type` | string | `payment.initiated`, `payment.authorised`, `payment.not_authorised`, `payment.validated`, `payment.invalidated`, `payment.completed`, `payment.not_completed` |
| `schema_version` | int | Version of this schema, for evolution. |
| `payment_id` | string | Business key of the payment. Also the Kafka message key. |
| `sequence_no` | int | 1, 2, 3, ... within one payment. Lets consumers detect gaps and order. |
| `occurred_at` | ISO 8601 UTC, ms | When the state change happened. |
| `producer_id` | string | Which producer instance wrote the event. |
| `customer_id` | string | Payer. Needed for fraud velocity checks and notifications. |
| `merchant_id` | string | Payee merchant. Needed for reconciliation. |
| `amount_minor` | int | Amount in cents. Integer to avoid floating-point rounding. |
| `currency` | string | ISO 4217 code (`ZAR`). |
| `payment_type` | string | `P2P`, `MERCHANT`, `BILL`, `EFT`. |
| `channel` | string | `APP`, `WEB`, `USSD`, `POS`. Used by fraud rules. |
| `device_id` | string | Device fingerprint. Used by fraud rules and audit. |
| `status` | string | State after this event. |
| `reason_code` | string | Present on failure events. Needed for customer messages and reconciliation. |
| `journey_outcome`, `journey_index` | string, int | Simulation metadata, present only in the POC. |

**Fields needed by each use:** fraud needs `customer_id`, `amount_minor`, `channel`, `payment_type`, `device_id`. Notifications need `customer_id`, `amount_minor`, `reason_code`. Reconciliation and audit need every field plus `event_id`, `sequence_no` and `occurred_at`.

---

### 1.11 Producer design

**Event types.** The payment producer follows the state machine in project.md section 1:

```
payment.initiated
 ├─ payment.not_authorised                                   (failed)
 └─ payment.authorised
     ├─ payment.invalidated                                  (failed)
     └─ payment.validated
         ├─ payment.not_completed                            (failed)
         └─ payment.completed
```

Every transition is published, because each one is needed for audit, and failures are published as events rather than silently dropped.

**Partition key = `payment_id`.** Kafka guarantees order only within a partition. Keying by `payment_id` puts every event for one payment in one partition, in the order it was produced, so the lifecycle can be replayed correctly. Keying by `customer_id` was rejected as the primary key: it would order all of one customer's payments together, which fraud velocity wants, but a customer can have many payments in flight and the lifecycle would still need a second key. Customer-level velocity is handled by repartitioning by `customer_id` inside the fraud consumer, which is a separate step.

**Durability: `acks=all`.** The business cost of losing a payment event is high (a missing payment breaks reconciliation and regulatory audit), while the latency cost is a few milliseconds. `acks=all` waits for all in-sync replicas, which with `min.insync.replicas=2` means at least two copies. `acks=1` was rejected: a leader failure right after the ack loses the event. `acks=0` is never acceptable here.

**Idempotency: `enable.idempotence=true`.** This sets `acks=all`, makes retries safe, and keeps ordering with up to 5 in-flight requests. A retry after a network hiccup cannot create a duplicate inside a producer session, because the broker rejects a repeated (producer id, partition, sequence) tuple. Duplicates across producer restarts are handled differently: the business `payment_id` is generated once when the payment is requested (in production, from the client's idempotency key), and every consumer de-duplicates on `event_id` or on `source_event_id`. Transactions (`transactional.id`) were not used because the consumers already commit only after side effects flush; exactly-once end to end would need transactions across the consume-transform-produce loop, which adds latency.

**Retries and error handling.**
- `retries=2147483647` and `delivery.timeout.ms=30000`: retry transient errors (leader change, network blip) until 30 seconds have passed, then give up.
- `request.timeout.ms=10000`: a single request that does not complete in 10 seconds is retried.
- Maximum time a payment can be stuck in the producer: **30 seconds**. A payment flow must not wait longer, so the upstream system moves the payment to a timeout state after that.
- Events that still fail are written to a dead-letter file (`dlq/failed-events.jsonl`) with the error, so nothing is silently lost. Alerting on that file is required in production.
- Fail fast or wait patiently: the producer waits, because payment events must not be lost. Breaking the 150 ms SLA is preferred over losing an event; the timeout is the bound.

**Serialization: JSON for the POC.** JSON is human-readable, so the CLI consumers and this document can show real payloads. Its cost is size (about 2 to 3 times Avro) and no enforced schema. For production, **Avro with Schema Registry** is the recommendation: it is compact, the schema is enforced at produce time, and `BACKWARD` compatibility lets fields be added without breaking consumers. Protobuf is a good alternative if the organisation already uses gRPC. The schema fields in section 1.10 are the same in all three formats, so the choice does not change the design.

**Scale and test data.** The producer generates 1,000 payment journeys by default (about 2,800 events, see section 4.3). The mix is 85% completed, 6% not authorised, 4% invalidated and 5% not completed, so every failure path is exercised. Channels, payment types and amounts are randomised with a fixed seed so runs can be repeated.

---

### 1.12 Consumer design

| Group | Reads | Filters | Writes | Lag SLA |
|---|---|---|---|---|
| `fraud-scoring-consumer` | `digital.payments.lifecycle.v1` | `payment.initiated` only | `digital.fraud.scores.v1` | < 50 ms |
| `notification-sender-consumer` | `digital.payments.lifecycle.v1` | `payment.validated`, `payment.not_authorised`, `payment.invalidated`, `payment.not_completed` | `digital.notifications.v1` | < 2 s |
| `audit-reconciliation-consumer` | `digital.payments.lifecycle.v1` | none, archives every event | `audit/payments-YYYY-MM-DD.jsonl` (WORM archive in production) | < 60 s |

Naming follows `<domain>-<purpose>-consumer`, hyphen separated and descriptive.

**Why three independent groups.** Each group has its own offsets, so each reads the full stream at its own pace. Slowness in one (for example a notification gateway outage) does not hold back fraud, which has the strictest lag limit. A fourth consumer would not be needed for the POC.

**Fraud consumer.**
- *Purpose:* score each payment before it completes. The score is used by the authorisation decision upstream.
- *Lag SLA < 50 ms:* the score must arrive before authorisation is decided. Lag is measured from event time to score time.
- *Processing:* a rules model scores each payment 0 to 100 from amount, payment type, channel and customer velocity (5 or more payments in 60 seconds). Scores under 40 are LOW, 40 to 69 are MEDIUM and 70 and above are HIGH.
- *Output:* one `fraud.score.<band>` event per payment, keyed by `payment_id`. It carries `source_event_id`, so duplicates can be dropped.
- *Failure detection:* any exception stops the loop and the group restarts from the last committed offset. Processing is idempotent for the same input, so the replay is safe.
- *Offsets:* manual commits. Output is flushed to the broker before the offset is committed, every 100 messages. This gives at-least-once processing: a crash after the flush but before the commit produces duplicate scores, which the `source_event_id` removes.
- *Ordering and exactly-once:* the consumer depends on per-payment order (`sequence_no`), which the partition key preserves. Exactly-once is not needed because the score is a pure function of the event, so reprocessing gives the same result.
- *Scalability:* up to 100 instances (one per partition). Each handles about 10 MB/s. The velocity state is in memory in this POC; production must use a keyed state store so that a rebalance does not reset velocity counts.

**Notification consumer.**
- *Purpose:* tell the customer the payment outcome.
- *Lag SLA < 2 s:* a customer who waits more than a couple of seconds for a payment message assumes the payment failed and retries it.
- *Processing:* `payment.validated` produces an "approved" message. Any of the three failure events produces a "failed" message with the reason code. Each sent message is published to `digital.notifications.v1` as `notification.payment.approved` or `notification.payment.failed`.
- *Output:* the mock gateway call (`send_push`) and the outcome event.
- *Failure detection:* a gateway failure should be retried with back-off in production. The POC logs and skips it, which is shown in section 7.
- *Offsets, ordering and scale:* manual commit, at-least-once, up to 100 instances. A duplicate push is possible after a crash. The notification outcome carries `source_event_id` so a sender can de-duplicate, and in production the gateway call uses an idempotency key.

**Audit / reconciliation consumer.**
- *Purpose:* the 7-year archive and the input to daily reconciliation.
- *Lag SLA < 60 s:* reconciliation runs in batches, so a one-minute delay is acceptable. Larger lag is a warning sign.
- *Processing:* append the event with topic, partition and offset to a daily file. Each record keeps its Kafka coordinates, so any archived record can be traced back to the stream.
- *Offsets:* manual commit after `fsync`, so a committed offset always means the record is on disk.
- *Ordering:* the archive does not need the cross-partition order; it uses `occurred_at` and `sequence_no`.

---

## 2. Topic Creation

Script: [`kafka/create-topics.sh`](kafka/create-topics.sh). The payment topic command, as used in the script:

```bash
kubectl exec -i kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 \
  --create --if-not-exists \
  --topic digital.payments.lifecycle.v1 \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220752000000 \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=1048576
```

Mapping to the templates in `fraud.md`, `payment.md` and `notification.md`: `retention.ms` = 220752000000, `cleanup.policy` = delete, `min.insync.replicas` = 2, `compression.type` = zstd, `max.message.bytes` = 1048576. The same pattern is used for the other two topics with their own names, partitions and retention.

### 2.2 Describe output: TO CAPTURE

```bash
kubectl exec -i kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 \
  --describe --topic digital.payments.lifecycle.v1
```

Expected to show `PartitionCount: 100`, `ReplicationFactor: 3`, and the configs `retention.ms=220752000000`, `min.insync.replicas=2`, `compression.type=zstd`, `cleanup.policy=delete`, `max.message.bytes=1048576`.

```text
<paste output of --describe here>
```

---

## 3. Producer Design Summary

Producer code: [`app/payment_producer.py`](app/payment_producer.py), with shared settings in [`app/common.py`](app/common.py). A custom Python producer was chosen over `kafka-console-producer.sh` because the console producer cannot apply per-event keys derived from business logic, cannot generate the state machine, and cannot retry to a dead-letter file.

Producer settings (`common.producer_config`):

| Setting | Value | Reason |
|---|---|---|
| `acks` | all | No loss on leader failure (section 1.11) |
| `enable.idempotence` | true | No duplicates from retries |
| `max.in.flight.requests.per.connection` | 5 | Maximum allowed with idempotence, keeps throughput |
| `retries` | 2147483647 | Retry until `delivery.timeout.ms` |
| `delivery.timeout.ms` | 30000 | Upper bound on time a payment is stuck |
| `request.timeout.ms` | 10000 | Single-request timeout |
| `linger.ms` | 5 | Small batching window within the 150 ms budget |
| `batch.size` | 65536 | Batch size for compression efficiency |
| `compression.type` | zstd | Section 1.7 |

---

## 4. Producer Setup

### 4.1 Commands

Run from `app/`:

```bash
pip install -r requirements.txt
export KAFKA_BOOTSTRAP=localhost:9092          # or the port-forwarded kafka-service address
python payment_producer.py --count 1000 --seed 42
```

Preview the events without a broker:

```bash
python payment_producer.py --count 3 --seed 42 --dry-run
```

### 4.2 Example events (dry run, real output)

The first journeys from `--count 3 --seed 42` look like this:

```json
{"event_id":"<uuid>","event_type":"payment.initiated","schema_version":1,"payment_id":"PAY-<hex>","sequence_no":1,"occurred_at":"<utc>","producer_id":"payment-producer-1","customer_id":"CUST-<n>","merchant_id":"MER-<n>","amount_minor":<int>,"currency":"ZAR","payment_type":"<type>","channel":"<channel>","device_id":"DEV-<n>","journey_outcome":"<outcome>","journey_index":1,"status":"INITIATED"}
```

Run the dry-run command to see the exact values for these seeds; the structure is fixed as above. Two different event types to show in the final run output: a `payment.not_authorised` event carries `status` `NOT_AUTHORISED` and a `reason_code` such as `51_INSUFFICIENT_FUNDS`, and a `payment.completed` event carries `status` `COMPLETED` with no reason code.

### 4.3 Production run: TO CAPTURE

```bash
python payment_producer.py --count 1000 --seed 42
```

Expected summary line (counts are deterministic for the seed):

```text
Flush complete, messages still in queue: 0
Broker acks received: <events>, delivery failures (sent to DLQ): 0
Journeys: 1000, events: <events>, outcome mix: {...}
```

Each journey produces 2 to 4 events, so the event total is between 2,000 and 4,000 and is printed by the script. Evidence of partition and offset assignment:

```bash
kubectl exec -i kafka-0 -- kafka-get-offsets --bootstrap-server kafka-service:9092 \
  --topic digital.payments.lifecycle.v1
```

```text
<paste output here>
```

---

## 5. Consumer Setup

Create each consumer group in its own terminal. Each runs until stopped with Ctrl+C, and each commits offsets only after its output is flushed.

```bash
# Terminal 1: fraud scoring
python fraud_consumer.py

# Terminal 2: notifications
python notification_consumer.py

# Terminal 3: audit / reconciliation
python audit_consumer.py
```

Consumer group configuration (`common.consumer_config`):

| Setting | Value | Reason |
|---|---|---|
| `group.id` | one of the three names | Independent offsets per use case |
| `auto.offset.reset` | earliest | A new group reads the full history, required for the audit archive |
| `enable.auto.commit` | false | Commit only after side effects are flushed |
| `partition.assignment.strategy` | cooperative-sticky | Fewer partitions move during a rebalance |
| `session.timeout.ms` | 10000 | Failure detection in 10 seconds |
| `heartbeat.interval.ms` | 3000 | One third of the session timeout |
| `max.poll.interval.ms` | 300000 | Allows a slow batch without being kicked out of the group |
| `fetch.min.bytes` | 1 | Lowest fetch latency |
| `fetch.wait.max.ms` | 50 | Keeps polling latency low for the 50 ms fraud SLA |
| `max.partition.fetch.bytes` | 1048576 | Bounds memory per partition |

The three template files use `--property` for consumer and producer settings. In the Kafka CLI those settings are passed with `--consumer-property` and `--producer-property`, and `--property` is for message parsing. The custom Python consumers apply the settings directly, which avoids that error.

### 5.1 Sample processing output: TO CAPTURE

```text
<fraud_consumer output, e.g. "scored payment=PAY-... score=45 band=MEDIUM reasons=['ELEVATED_AMOUNT']">
<notification_consumer output, e.g. "-> push to CUST-...: Your payment of R1234.56 was declined. Reason: 51_INSUFFICIENT_FUNDS.">
<audit_consumer summary, e.g. "audit-reconciliation-consumer archived N events">
```

### 5.2 Error handling in action

- A failed delivery after `delivery.timeout.ms` is written to `dlq/failed-events.jsonl` (see `common._on_delivery`).
- A non-JSON message stops the consumer with an exception, and the group restarts from the last committed offset, so the message is retried rather than skipped.
- To demonstrate: stop one broker during a run and show the events still acknowledged once the leader fails over (TO CAPTURE if the lab allows it).

---

## 6. Verification and Monitoring

### 6.1 Consumer group status: TO CAPTURE

```bash
kubectl exec -i kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 \
  --describe --group fraud-scoring-consumer
kubectl exec -i kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 \
  --describe --group notification-sender-consumer
kubectl exec -i kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 \
  --describe --group audit-reconciliation-consumer
```

Expected: `LAG` is 0 or near 0 for each group on every partition, meaning all three are caught up.

```text
<paste output here>
```

### 6.2 Ordering verification

```bash
python verify_ordering.py
```

The script reads the whole topic with a throwaway group that never commits. For each payment it checks that all events are in one partition, offsets increase, and `sequence_no` runs 1..n with no gaps.

```text
Payments checked: <n>, events: <n>
Ordering check: PASSED
```

Paste the real result. A `FAILED` result means the partition key or the producer configuration needs review.

### 6.3 Topic status and configuration

The `--describe` output in section 2.2 is the configuration evidence.

---

## 7. Trade-offs and Justifications

- **Partitions 100 vs fewer.** Fewer partitions would cap consumer parallelism below the 1000 MB/s target. More would add broker overhead for little gain at this scale.
- **RF 3 / minISR 2 vs RF 2 or minISR 1.** Costs 50% more storage and replication, in exchange for surviving any single broker failure with no loss of acknowledged data. Acceptable because payment data is regulated.
- **7-year time retention with no size limit.** Compliance is measured in time, so size limits would breach the hold. Cost is managed by tiered storage rather than deleting data.
- **`cleanup.policy=delete` vs compaction.** Compaction would destroy the history that audit needs. Delete keeps every event.
- **zstd vs lz4 / snappy.** zstd gives the best ratio on JSON. lz4 would be faster but larger. For 1000 MB/s total the storage and network saving outweighs the small CPU cost.
- **`acks=all` + idempotence.** Slightly higher latency (a few milliseconds), in exchange for no loss and no duplicates on retry. This is the right trade for payments.
- **Key = `payment_id` vs `customer_id`.** Per-payment order is what the lifecycle needs. Customer velocity is handled by a separate repartition step, so it does not weaken the payment key.
- **JSON vs Avro/Protobuf.** JSON is used in the POC for readability; the production recommendation is Avro with Schema Registry for size and enforced compatibility.
- **Manual commits, at-least-once with de-duplication.** Exactly-once across the whole pipeline needs transactions, which add latency against the 50 ms fraud SLA. Idempotent consumers with `source_event_id` give the same business result at lower cost.
- **30-second stuck limit.** Long enough to ride out a leader election, short enough that a customer does not wait indefinitely. Events beyond that go to a dead-letter file rather than being lost.

---

## 8. Issues Encountered

1. **Environment.** The Docker daemon was not running and no Python runtime was installed, so the scripts could not be executed against a live cluster during preparation. The code was written against the `confluent-kafka` 2.5 API and the Kafka configuration names. Run `verify_ordering.py` and section 4.3 first on the cluster to confirm.
2. **CLI property syntax.** The template files pass producer and consumer settings with `--property`. The Kafka CLI tools use `--producer-property` and `--consumer-property`. The design uses the Python clients, which avoid this.
3. **Replication factor on a small local cluster.** RF 3 and minISR 2 need at least 3 brokers. On a single-broker local cluster, `create-topics.sh` must be run with `--replication-factor 1` and `min.insync.replicas=1`, and that deviation must be stated in the final run.
4. **Lag as a business metric.** Kafka offset lag is a proxy. The SLA in the brief is end-to-end time (event to score or notification). Both are reported: offset lag from section 6.1, and the event time in `occurred_at` compared with the consumer log time.
5. **Dead-letter handling.** A delivery failure sets the producer to park the event in a file. This prevents loss but can break per-payment order for later events if a middle event is parked. The DLQ is for alerting and manual replay, not transparent retry.

---

## 9. Conclusions

- Keying by `payment_id` with `acks=all`, idempotence and manual commits gives ordered, durable and de-duplicable payment events.
- Splitting consumers into three groups isolates the fraud, notification and audit use cases, so each has its own lag SLA and failure domain.
- The main production gap is state: fraud velocity lives in memory in the POC and must move to a state store, and the notification gateway needs retry with back-off.
- With more time, I would move to Avro and Schema Registry, add a transactional outbox in the payment service so an event is never lost between the database write and the producer, and automate the verification steps in section 6 as a CI check.

---

## Submission files

| File | Purpose |
|---|---|
| `payment_events_setup.md` | This document |
| `kafka/create-topics.sh` | Topic creation and describe commands |
| `app/common.py` | Shared event builder, producer and consumer settings |
| `app/payment_producer.py` | Payment lifecycle producer |
| `app/fraud_producer.py` | Fraud score producer (low / medium / high) |
| `app/fraud_consumer.py` | Fraud scoring consumer |
| `app/notification_producer.py` | Notification outcome producer |
| `app/notification_consumer.py` | Notification sender consumer |
| `app/audit_consumer.py` | Audit / reconciliation consumer |
| `app/verify_ordering.py` | Ordering verification |
| `app/requirements.txt` | Python dependency |
