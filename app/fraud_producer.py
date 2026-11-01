"""Fraud producer: publishes fraud score events (low / medium / high) to its own topic."""
from common import TOPIC_FRAUD, build_event, make_producer, publish

PRODUCER_ID = "fraud-producer-1"

# event_type per score band, as required by project.md section 3.
EVENT_TYPE_BY_BAND = {
    "LOW": "fraud.score.low",
    "MEDIUM": "fraud.score.medium",
    "HIGH": "fraud.score.high",
}


class FraudProducer:
    def __init__(self):
        self._producer = make_producer(PRODUCER_ID)

    def publish_score(self, payment_event, score, band, reasons):
        event = build_event(
            EVENT_TYPE_BY_BAND[band],
            payment_event["payment_id"],
            sequence_no=payment_event["sequence_no"],
            producer_id=PRODUCER_ID,
            source_event_id=payment_event["event_id"],  # lets downstream consumers de-duplicate
            customer_id=payment_event["customer_id"],
            amount_minor=payment_event["amount_minor"],
            currency=payment_event["currency"],
            fraud_score=score,
            score_band=band,
            reasons=reasons,
            model_version="rules-v1",
        )
        publish(self._producer, TOPIC_FRAUD, payment_event["payment_id"], event)

    def flush(self):
        self._producer.flush(30)
