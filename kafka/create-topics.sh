#!/usr/bin/env bash
# Creates the three topics used by the payment event POC.
# Production-sized values are used; see payment_events_setup.md section 1 for the reasoning.
set -euo pipefail

BOOTSTRAP="kafka-service:9092"
KT="kubectl exec -i kafka-0 -- kafka-topics --bootstrap-server ${BOOTSTRAP}"

SEVEN_YEARS_MS=220752000000   # 7 * 365 days
THIRTY_DAYS_MS=2592000000

# Production defaults are 100 / 100 / 50 partitions. Local POC clusters with 1 GiB brokers can OOM
# at that size, so override with PAYMENT_PARTITIONS / FRAUD_PARTITIONS / NOTIFICATION_PARTITIONS.
PAYMENT_PARTITIONS=${PAYMENT_PARTITIONS:-100}
FRAUD_PARTITIONS=${FRAUD_PARTITIONS:-100}
NOTIFICATION_PARTITIONS=${NOTIFICATION_PARTITIONS:-50}

# 1. Payment lifecycle events (regulatory 7-year hold, 100 partitions for 1000 MB/s at 10 MB/s per consumer)
$KT --create --if-not-exists \
  --topic digital.payments.lifecycle.v1 \
  --partitions ${PAYMENT_PARTITIONS} \
  --replication-factor 3 \
  --config retention.ms=${SEVEN_YEARS_MS} \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=1048576

# 2. Fraud scores (regulatory 7-year hold, same partition count so fraud output joins payments by key)
$KT --create --if-not-exists \
  --topic digital.fraud.scores.v1 \
  --partitions ${FRAUD_PARTITIONS} \
  --replication-factor 3 \
  --config retention.ms=${SEVEN_YEARS_MS} \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=1048576

# 3. Notification outcomes (operational, 30-day replay window)
$KT --create --if-not-exists \
  --topic digital.notifications.v1 \
  --partitions ${NOTIFICATION_PARTITIONS} \
  --replication-factor 3 \
  --config retention.ms=${THIRTY_DAYS_MS} \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=1048576

# Verification
$KT --describe --topic digital.payments.lifecycle.v1
$KT --describe --topic digital.fraud.scores.v1
$KT --describe --topic digital.notifications.v1
