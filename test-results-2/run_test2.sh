#!/usr/bin/env bash
S="$1"; L2="$S/livetest2"; R2="$S/output/test-results-2"; PY="$LOCALAPPDATA/Programs/Python/Python312/python.exe"
export KAFKA_BOOTSTRAP=localhost:30092,localhost:30093,localhost:30094 DLQ_PATH=dlq/failed-events.jsonl AUDIT_DIR=audit PYTHONIOENCODING=utf-8
cd "$L2"
G="kubectl exec kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092"
timeout 1200 "$PY" fraud_consumer.py > "$R2/fraud_consumer.log" 2>&1 &
timeout 1200 "$PY" notification_consumer.py > "$R2/notification_consumer.log" 2>&1 &
timeout 1200 "$PY" audit_consumer.py > "$R2/audit_consumer.log" 2>&1 &
for i in $(seq 1 12); do
  ok=0
  for g in fraud-scoring-consumer notification-sender-consumer audit-reconciliation-consumer; do
    $G --describe --state --group $g 2>/dev/null | grep -q "Stable" && ok=$((ok+1))
  done
  echo "[$i] stable groups: $ok/3" >> "$R2/group_join.txt"
  [ "$ok" = "3" ] && break
  sleep 5
done
"$PY" payment_producer.py --count 60 --seed 42 > "$R2/producer.log" 2>&1
echo "producer done" >> "$R2/group_join.txt"
for i in $(seq 1 20); do
  bad=0
  for g in fraud-scoring-consumer notification-sender-consumer audit-reconciliation-consumer; do
    b=$($G --describe --group $g 2>/dev/null | awk 'NR>1 && ($6=="-" || $6>0){b++} END{print b+0}')
    bad=$((bad+b))
  done
  echo "[$i] lagging or unassigned partitions: $bad" >> "$R2/lag_wait.txt"
  [ "$bad" = "0" ] && break
  sleep 10
done
for g in fraud-scoring-consumer notification-sender-consumer audit-reconciliation-consumer; do
  $G --describe --group $g > "$R2/lag_$g.txt" 2>&1
done
KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP "$PY" verify_ordering.py > "$R2/ordering_check.txt" 2>&1
echo DONE > "$R2/done.flag"
