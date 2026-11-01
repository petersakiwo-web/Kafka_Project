"""Verifies ordering: every payment's events are in one partition, offsets increase, and
sequence_no runs 1..n with no gaps. Reads the topic from the beginning with a throwaway group
that never commits, so it does not disturb the real consumer groups.

Usage: python verify_ordering.py
"""
import json
import uuid
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError

from common import BOOTSTRAP, TOPIC_PAYMENTS

IDLE_TIMEOUT_S = 10


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"ordering-verifier-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_PAYMENTS])

    by_payment = defaultdict(list)
    idle = 0.0
    while idle < IDLE_TIMEOUT_S:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1.0
            continue
        idle = 0.0
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(msg.error())
            continue
        event = json.loads(msg.value())
        by_payment[event["payment_id"]].append((msg.partition(), msg.offset(), event))
    consumer.close()

    problems = []
    for payment_id, rows in by_payment.items():
        partitions = {p for p, _, _ in rows}
        if len(partitions) != 1:
            problems.append(f"{payment_id}: spread over partitions {sorted(partitions)}")
        offsets = [o for _, o, _ in rows]
        if offsets != sorted(offsets):
            problems.append(f"{payment_id}: offsets out of order")
        seqs = [e["sequence_no"] for _, _, e in rows]
        if seqs != list(range(1, len(seqs) + 1)):
            problems.append(f"{payment_id}: sequence_no {seqs}")

    print(f"Payments checked: {len(by_payment)}, events: {sum(len(r) for r in by_payment.values())}")
    print("Ordering check:", "PASSED" if not problems else f"FAILED ({len(problems)} problems)")
    for line in problems[:20]:
        print("  ", line)


if __name__ == "__main__":
    main()
