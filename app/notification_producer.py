"""Notification producer: publishes notification outcome events (payment approved / failed)."""
from common import TOPIC_NOTIFICATIONS, build_event, make_producer, publish

PRODUCER_ID = "notification-producer-1"

EVENT_TYPE_BY_OUTCOME = {
    "APPROVED": "notification.payment.approved",
    "FAILED": "notification.payment.failed",
}


class NotificationProducer:
    def __init__(self):
        self._producer = make_producer(PRODUCER_ID)

    def publish_notification(self, payment_event, outcome, message, channel="PUSH"):
        event = build_event(
            EVENT_TYPE_BY_OUTCOME[outcome],
            payment_event["payment_id"],
            sequence_no=payment_event["sequence_no"],
            producer_id=PRODUCER_ID,
            source_event_id=payment_event["event_id"],
            customer_id=payment_event["customer_id"],
            channel=channel,
            message=message,
            delivery_status="SENT",
        )
        publish(self._producer, TOPIC_NOTIFICATIONS, payment_event["payment_id"], event)

    def flush(self):
        self._producer.flush(30)
