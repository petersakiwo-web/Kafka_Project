"""Audit / reconciliation consumer: archives every payment lifecycle event for the 7-year hold.

Group: audit-reconciliation-consumer   SLA: event-to-archive < 60 s
Output: audit/payments-YYYY-MM-DD.jsonl (stand-in for WORM / object-storage archive)
"""
import json
import os
from datetime import datetime, timezone

from confluent_kafka import KafkaError, KafkaException

from common import TOPIC_PAYMENTS, commit_batch, make_consumer

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
    last_msg = None
    out = open(archive_path(), "a", encoding="utf-8")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
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
            last_msg = msg

            if archived % COMMIT_EVERY == 0:
                out.flush()
                os.fsync(out.fileno())
                commit_batch(consumer, last_msg)
    except KeyboardInterrupt:
        pass
    finally:
        out.flush()
        os.fsync(out.fileno())
        out.close()
        commit_batch(consumer, last_msg)
        consumer.close()
        print(f"{GROUP} archived {archived} events")


if __name__ == "__main__":
    run()
