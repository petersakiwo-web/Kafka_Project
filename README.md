# Kafka Payment Events POC

A proof of concept for real-time payment event streaming on Kafka. It shows how payment lifecycle events feed fraud scoring, customer notifications and a 7-year audit archive, each as an independent consumer group.

## What is included

| Path | Purpose |
|---|---|
| `payment_events_setup.md` | Full design document: topic, producer and consumer decisions, commands, trade-offs and issues |
| `kafka/create-topics.sh` | Creates and describes the three topics |
| `app/common.py` | Shared event builder, durable producer and consumer settings |
| `app/payment_producer.py` | Simulates payment journeys (initiated → completed / failed) |
| `app/fraud_producer.py` | Publishes fraud scores: `fraud.score.low`, `.medium`, `.high` |
| `app/fraud_consumer.py` | Scores `payment.initiated` events in real time |
| `app/notification_producer.py` | Publishes `notification.payment.approved` / `.failed` |
| `app/notification_consumer.py` | Sends customer notifications for validated and failed payments |
| `app/audit_consumer.py` | Archives every payment event for the 7-year hold |
| `app/verify_ordering.py` | Checks that each payment's events are in one partition, in order |

## Topics

| Topic | Partitions | Retention | Producer |
|---|---|---|---|
| `digital.payments.lifecycle.v1` | 100 | 7 years | payment-producer |
| `digital.fraud.scores.v1` | 100 | 7 years | fraud-producer |
| `digital.notifications.v1` | 50 | 30 days | notification-producer |

All topics use replication factor 3, `min.insync.replicas=2`, `cleanup.policy=delete` and zstd compression.

## Key design choices

- **Ordering:** events are keyed by `payment_id`, so each payment's lifecycle stays in one partition in order.
- **Durability:** `acks=all` with idempotence. Events that cannot be delivered within 30 seconds go to a dead-letter file.
- **Consumers:** three independent groups, each with manual offset commits, so a slow consumer does not hold back the others.

## Quick start

```bash
# 1. Create topics (requires the local Kafka cluster)
bash kafka/create-topics.sh

# 2. Install the client library
cd app && pip install -r requirements.txt

# 3. Start the consumers (separate terminals)
python fraud_consumer.py
python notification_consumer.py
python audit_consumer.py

# 4. Produce 1,000 payment journeys
python payment_producer.py --count 1000 --seed 42

# 5. Verify ordering
python verify_ordering.py
```

To preview events without a broker, run `python payment_producer.py --count 3 --dry-run`.

Set `KAFKA_BOOTSTRAP` to point at your cluster (default `kafka-service:9092`).

## Status

The code and documentation are complete. Live run evidence (topic describe output, producer counts, consumer lag and the ordering check) has not been captured yet. See the **TO CAPTURE** sections in `payment_events_setup.md`.
