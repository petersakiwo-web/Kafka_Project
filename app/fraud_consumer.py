"""Fraud consumer: consumes payment.initiated events, runs real-time fraud scoring,
and hands each result to the fraud producer.

Group: fraud-scoring-consumer   SLA: consume-to-score < 50 ms
"""
import json
import time
from collections import defaultdict, deque

from confluent_kafka import KafkaError, KafkaException

from common import COMMIT_INTERVAL_S, TOPIC_PAYMENTS, commit_offsets, make_consumer, track_offset
from fraud_producer import FraudProducer

GROUP = "fraud-scoring-consumer"
WANTED_EVENT = "payment.initiated"
COMMIT_EVERY = 100  # flush fraud output, then commit offsets, every 100 messages

# In-memory velocity window per customer. In production this state lives in a keyed state store
# (for example RocksDB-backed or Redis) so it survives restarts and rebalances.
VELOCITY = defaultdict(deque)


def score(event):
    points = 0
    reasons = []

    amount = event["amount_minor"]
    if amount >= 300000:
        points += 35
        reasons.append("HIGH_AMOUNT")
    elif amount >= 100000:
        points += 15
        reasons.append("ELEVATED_AMOUNT")

    if event["payment_type"] == "EFT":
        points += 10
        reasons.append("EFT_TRANSFER")
    if event["channel"] in ("USSD", "WEB"):
        points += 10
        reasons.append("LOWER_TRUST_CHANNEL")

    now = time.time()
    window = VELOCITY[event["customer_id"]]
    window.append(now)
    while window and now - window[0] > 60:
        window.popleft()
    if len(window) >= 5:
        points += 30
        reasons.append("VELOCITY_5_PER_MIN")

    if points >= 70:
        band = "HIGH"
    elif points >= 40:
        band = "MEDIUM"
    else:
        band = "LOW"
    return points, band, reasons


def run():
    consumer = make_consumer(GROUP)
    consumer.subscribe([TOPIC_PAYMENTS])
    fraud = FraudProducer()
    processed = 0
    pending = {}  # partition -> next offset, for messages read but not yet committed
    last_commit = time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if pending and time.monotonic() - last_commit >= COMMIT_INTERVAL_S:
                    fraud.flush()
                    commit_offsets(consumer, pending)
                    pending.clear()
                    last_commit = time.monotonic()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            event = json.loads(msg.value())
            if event["event_type"] == WANTED_EVENT:
                points, band, reasons = score(event)
                fraud.publish_score(event, points, band, reasons)
                processed += 1
                print(f"scored payment={event['payment_id']} score={points} band={band} reasons={reasons}", flush=True)

            track_offset(pending, msg)  # filtered-out events are also marked done
            if processed and processed % COMMIT_EVERY == 0:
                fraud.flush()
                commit_offsets(consumer, pending)
                pending.clear()
                last_commit = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        fraud.flush()
        commit_offsets(consumer, pending)
        consumer.close()
        print(f"{GROUP} stopped after scoring {processed} payments", flush=True)


if __name__ == "__main__":
    run()
