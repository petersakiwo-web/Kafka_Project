"""Notification consumer: consumes payment.validated and payment failure events,
sends the customer notification (mocked here) and publishes the outcome.

Group: notification-sender-consumer   SLA: event-to-notification < 2 s
"""
import json
import time

from confluent_kafka import KafkaError, KafkaException

from common import COMMIT_INTERVAL_S, TOPIC_PAYMENTS, commit_offsets, make_consumer, track_offset
from notification_producer import NotificationProducer

GROUP = "notification-sender-consumer"
COMMIT_EVERY = 100

# Events that drive a customer notification.
APPROVED_EVENTS = {"payment.validated": "Your payment of R{amount} has been approved."}
FAILED_EVENTS = {
    "payment.not_authorised": "Your payment of R{amount} was declined. Reason: {reason}.",
    "payment.invalidated": "Your payment of R{amount} could not be validated. Reason: {reason}.",
    "payment.not_completed": "Your payment of R{amount} did not complete. Reason: {reason}.",
}


def send_push(customer_id, text):
    """Mock of the push / SMS gateway call. A real sender would retry with back-off."""
    print(f"  -> push to {customer_id}: {text}", flush=True)
    return True


def build_message(event):
    amount = f"{event['amount_minor'] / 100:.2f}"
    reason = event.get("reason_code", "N/A")
    if event["event_type"] in APPROVED_EVENTS:
        return "APPROVED", APPROVED_EVENTS[event["event_type"]].format(amount=amount)
    if event["event_type"] in FAILED_EVENTS:
        return "FAILED", FAILED_EVENTS[event["event_type"]].format(amount=amount, reason=reason)
    return None, None


def run():
    consumer = make_consumer(GROUP)
    consumer.subscribe([TOPIC_PAYMENTS])
    notifier = NotificationProducer()
    processed = 0
    pending = {}
    last_commit = time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if pending and time.monotonic() - last_commit >= COMMIT_INTERVAL_S:
                    notifier.flush()
                    commit_offsets(consumer, pending)
                    pending.clear()
                    last_commit = time.monotonic()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            event = json.loads(msg.value())
            outcome, text = build_message(event)
            if outcome is not None:
                if send_push(event["customer_id"], text):
                    notifier.publish_notification(event, outcome, text)
                processed += 1

            track_offset(pending, msg)
            if processed and processed % COMMIT_EVERY == 0:
                notifier.flush()
                commit_offsets(consumer, pending)
                pending.clear()
                last_commit = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        notifier.flush()
        commit_offsets(consumer, pending)
        consumer.close()
        print(f"{GROUP} stopped after sending {processed} notifications", flush=True)


if __name__ == "__main__":
    run()
