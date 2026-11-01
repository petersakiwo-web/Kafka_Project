"""Payment producer: simulates digital payment journeys and publishes lifecycle events.

Journey rules (project.md section 1):
  payment.initiated
    -> payment.authorised | payment.not_authorised
       if authorised:
         -> payment.validated | payment.invalidated
            if validated:
              -> payment.completed | payment.not_completed

Usage:
  python payment_producer.py --count 1000 --seed 42            # publish to Kafka
  python payment_producer.py --count 3 --seed 42 --dry-run     # print events, no broker needed
"""
import argparse
import json
import random
import uuid

from common import TOPIC_PAYMENTS, DELIVERED, build_event, make_producer, publish

PRODUCER_ID = "payment-producer-1"
CHANNELS = ["APP", "WEB", "USSD", "POS"]
PAYMENT_TYPES = ["P2P", "MERCHANT", "BILL", "EFT"]

# Outcome mix for the simulated traffic (must sum to 100).
OUTCOMES = [
    ("completed", 85),
    ("not_authorised", 6),
    ("invalidated", 4),
    ("not_completed", 5),
]

REASON_CODES = {
    "not_authorised": ["51_INSUFFICIENT_FUNDS", "61_LIMIT_EXCEEDED", "05_DO_NOT_HONOUR"],
    "invalidated": ["INVALID_BENEFICIARY", "ACCOUNT_CLOSED"],
    "not_completed": ["SETTLEMENT_TIMEOUT", "CORE_BANKING_UNAVAILABLE"],
}


def pick_outcome(rng):
    roll = rng.randint(1, 100)
    total = 0
    for name, weight in OUTCOMES:
        total += weight
        if roll <= total:
            return name
    return "completed"


def journey_steps(outcome):
    """Return the ordered list of (event_type, status, reason) for an outcome."""
    steps = [("payment.initiated", "INITIATED", None)]
    if outcome == "not_authorised":
        steps.append(("payment.not_authorised", "NOT_AUTHORISED", "REASON"))
        return steps
    steps.append(("payment.authorised", "AUTHORISED", None))
    if outcome == "invalidated":
        steps.append(("payment.invalidated", "INVALIDATED", "REASON"))
        return steps
    steps.append(("payment.validated", "VALIDATED", None))
    if outcome == "not_completed":
        steps.append(("payment.not_completed", "NOT_COMPLETED", "REASON"))
        return steps
    steps.append(("payment.completed", "COMPLETED", None))
    return steps


def generate_journey(rng, payment_index):
    payment_id = f"PAY-{uuid.UUID(int=rng.getrandbits(128)).hex[:12].upper()}"
    outcome = pick_outcome(rng)
    base = {
        "customer_id": f"CUST-{rng.randint(1, 50000):06d}",
        "merchant_id": f"MER-{rng.randint(1, 2000):05d}",
        "amount_minor": rng.randint(100, 500000),  # ZAR cents, integer to avoid float rounding
        "currency": "ZAR",
        "payment_type": rng.choice(PAYMENT_TYPES),
        "channel": rng.choice(CHANNELS),
        "device_id": f"DEV-{rng.randint(1, 90000):05d}",
        "journey_outcome": outcome,
        "journey_index": payment_index,
    }
    events = []
    for seq, (event_type, status, reason) in enumerate(journey_steps(outcome), start=1):
        fields = dict(base, status=status)
        if reason:
            fields["reason_code"] = rng.choice(REASON_CODES[event_type.split(".", 1)[1]])
        events.append(build_event(event_type, payment_id, seq, PRODUCER_ID, **fields))
    return payment_id, events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000, help="number of payment journeys")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dry-run", action="store_true", help="print events to stdout, do not publish")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    producer = None if args.dry_run else make_producer(PRODUCER_ID)
    total_events = 0
    outcome_counts = {}

    for index in range(1, args.count + 1):
        payment_id, events = generate_journey(rng, index)
        outcome = events[0]["journey_outcome"]
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        for event in events:
            total_events += 1
            if args.dry_run:
                print(json.dumps(event, separators=(",", ":")))
            else:
                # Key = payment_id: all events of one payment land in one partition, in order.
                publish(producer, TOPIC_PAYMENTS, payment_id, event)
        if producer is not None and index % 500 == 0:
            producer.poll(0)

    if producer is not None:
        remaining = producer.flush(60)
        print(f"Flush complete, messages still in queue: {remaining}")
        print(f"Broker acks received: {DELIVERED['ok']}, delivery failures (sent to DLQ): {DELIVERED['failed']}")

    print(f"Journeys: {args.count}, events: {total_events}, outcome mix: {outcome_counts}")


if __name__ == "__main__":
    main()
