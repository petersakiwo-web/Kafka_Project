"""Audit / reconciliation consumer: archives every payment lifecycle event for the 7-year hold.

Group: audit-reconciliation-consumer   SLA: event-to-archive < 60 s
Output: audit/payments-YYYY-MM-DD.jsonl (stand-in for WORM / object-storage archive)
"""
import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaError, KafkaException

from common import COMMIT_INTERVAL_S, TOPIC_PAYMENTS, commit_offsets, make_consumer, track_offset

GROUP = "audit-reconciliation-consumer"
AUDIT_DIR = os.getenv("AUDIT_DIR", "audit")
COMMIT_EVERY = 500


def archive_path():
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(AUDIT_DIR, f"payments-{day}.jsonl")


def run():
    consumer = make_consumer(GROUP)
    consumer.subscribe([TOPIC_PAYMENTS])
    os.makedirs(AUDIT_DIR, exist_ok=True)
    archived = 0
    pending = {}
    last_commit = time.monotonic()
    out = open(archive_path(), "a", encoding="utf-8")

    def flush_and_commit():
        out.flush()
        os.fsync(out.fileno())  # the record is on disk before its offset is committed
        commit_offsets(consumer, pending)
        pending.clear()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if pending and time.monotonic() - last_commit >= COMMIT_INTERVAL_S:
                    flush_and_commit()
                    last_commit = time.monotonic()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            record = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "event": json.loads(msg.value()),
            }
            out.write(json.dumps(record, separators=(",", ":")) + "\n")
            archived += 1
            track_offset(pending, msg)

            if archived % COMMIT_EVERY == 0:
                flush_and_commit()
                last_commit = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        flush_and_commit()
        out.close()
        consumer.close()
        print(f"{GROUP} archived {archived} events", flush=True)


if __name__ == "__main__":
    run()
