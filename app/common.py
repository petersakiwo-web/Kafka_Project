"""Shared configuration, event builder and producer helpers for the payment event POC."""
import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092")

TOPIC_PAYMENTS = "digital.payments.lifecycle.v1"
TOPIC_FRAUD = "digital.fraud.scores.v1"
TOPIC_NOTIFICATIONS = "digital.notifications.v1"

SCHEMA_VERSION = 1
DLQ_PATH = os.getenv("DLQ_PATH", "dlq/failed-events.jsonl")

# Counts successful broker acknowledgements per producer process, used for evidence output.
DELIVERED = {"ok": 0, "failed": 0}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event(event_type, payment_id, sequence_no, producer_id, **fields):
    """Every event is self-describing: it carries its own identifiers, time and schema version."""
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "payment_id": payment_id,
        "sequence_no": sequence_no,
        "occurred_at": utc_now(),
        "producer_id": producer_id,
    }
    event.update(fields)
    return event


def producer_config(client_id):
    """Durability and idempotency settings for every producer in the system (see design doc, section 4.3)."""
    return {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 2147483647,
        "delivery.timeout.ms": 30000,
        "request.timeout.ms": 10000,
        "linger.ms": 5,
        "batch.size": 65536,
        "compression.type": "zstd",
    }


def consumer_config(group_id):
    """Manual offset commit: a message is only marked done after its side effects are flushed."""
    return {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
        "max.poll.interval.ms": 300000,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 50,
        "max.partition.fetch.bytes": 1048576,
    }


def make_producer(client_id):
    return Producer(producer_config(client_id))


def make_consumer(group_id):
    return Consumer(consumer_config(group_id))


def _on_delivery(topic, key, event):
    def callback(err, msg):
        if err is None:
            DELIVERED["ok"] += 1
            return
        DELIVERED["failed"] += 1
        # Retries are exhausted (delivery.timeout.ms). Park the event so it is never silently lost.
        os.makedirs(os.path.dirname(DLQ_PATH) or ".", exist_ok=True)
        with open(DLQ_PATH, "a", encoding="utf-8") as dlq:
            dlq.write(json.dumps({"topic": topic, "key": key, "error": str(err), "event": event}) + "\n")

    return callback


def publish(producer, topic, key, event):
    payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=payload,
                             on_delivery=_on_delivery(topic, key, event))
            break
        except BufferError:
            # Local queue full: apply back-pressure instead of dropping the event.
            producer.poll(0.1)
    producer.poll(0)


def commit_batch(consumer, last_msg):
    """Commit the offset of the last processed message (Kafka commits are cumulative per partition)."""
    if last_msg is not None:
        consumer.commit(message=last_msg, asynchronous=False)
