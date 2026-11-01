# Live Test Summary: Kafka Payment Events POC

**Environment:** Rancher Desktop Kubernetes, 3 Kafka brokers (`kafka-0`, `kafka-1`, `kafka-2`), accessed from the host via NodePorts `localhost:30092-30094`.
**Test size:** 60 payment journeys, 235 events.

## Results

| Check | Result | Evidence |
|---|---|---|
| Topics created with the designed config | PASS | `topic_describe_payments.txt` (100 partitions, RF 3, minISR 2, zstd, delete, 7-year retention) |
| Producer delivered all events | PASS | `producer_and_ordering_run.log`: 235 broker acks, 0 DLQ entries |
| Event ordering per payment | PASS | `ordering_check.txt`: 60 payments, 235 events, one partition each, offsets increasing, `sequence_no` 1..n |
| Audit / reconciliation consumer archives every event | PASS (with duplicates, see issue 3) | `audit_archive_check.txt`: 235 unique events, 464 lines |
| Fraud consumer scores payments | FAIL | `fraud_consumer_run1.log`, `fraud_consumer_run3.log`: no scores produced, group never joined |
| Notification consumer sends notifications | FAIL | `notification_consumer_run3.log`: no output, group never joined |
| Consumer lag is near zero | NOT VERIFIED | `lag_fraud.txt`, `lag_notification.txt` failed (broker container restart); `lag_audit.txt` shows the audit group |

## Issues found and resolution steps

### 1. Docker engine down, no Python
- **Found:** `docker` could not reach the daemon and `python` was not installed.
- **Resolution:** Installed Python 3.12 for the user, then `confluent-kafka` 2.5.0 from `requirements.txt`. Started Rancher Desktop; its Kubernetes cluster and Kafka pods were then reachable.

### 2. Topic names collided with an existing cluster
- **Found:** The cluster already has `digitalpayments.payment.lifecycle`, `digitalpayments.payment.compact` and related topics. Those belong to someone else.
- **Resolution:** Left them alone. The project topics use the `digital.*.v1` names and were created as new topics. Nothing was deleted or altered.

### 3. Duplicate archive records after an interrupted consumer run
- **Found:** The audit archive has 464 lines for 235 events. The first audit run was stopped with SIGINT before its final commit, so the second run re-read the uncommitted records.
- **Resolution:** Confirmed as at-least-once delivery, which the design expects. The archive should be de-duplicated on `event_id` when reconciling. Production should use a graceful shutdown that commits before exit. The duplicate count is documented here and was not hidden.

### 4. Windows does not deliver SIGINT through `timeout`
- **Found:** Consumers stopped with `timeout -s INT` exited with code 124 and did not run their `finally` block. The batch commit (every 100 messages) was never reached for 60 journeys, so no offsets were committed.
- **Resolution:** Documented. Consumers should commit on a timer as well as on a count in production. Not yet changed in code.

### 5. Consumer group coordinator not ready
- **Found:** The producer and the fraud consumer logged `Coordinator load in progress`. The fraud and notification groups never joined, so they received no messages. The audit group, on another coordinator partition, did work.
- **Resolution:** Retried three times over about 10 minutes. The groups still did not join. The cluster was restarted by Kubernetes several times during the session (restart counts rose from 8 to 9 on `kafka-1`/`kafka-2` and 8 to 9 on `kafka-0`). This is the most likely cause, but it is not proven.
- **Status:** Open. Next step is to check the broker logs for `__consumer_offsets` load errors and rerun the fraud and notification consumers once the cluster is stable.

### 6. Lag commands failed
- **Found:** `kafka-consumer-groups --describe` for the fraud and notification groups returned `container not found ("kafka")` with exit code 137, meaning the pod was killed.
- **Resolution:** Logged as a failed check. Lag for the audit group was captured in `lag_audit.txt`. Lag for the other two groups must be measured after issue 5 is resolved.

### 7. Documentation: CLI property syntax (carried over from the design doc)
- **Found:** The template `--property` flags for `kafka-console-producer` and `kafka-console-consumer` are not valid; the CLI uses `--producer-property` and `--consumer-property`.
- **Resolution:** The custom Python clients were used instead, which avoids the problem. The design doc already notes this.

## Not done
- Fraud and notification consumers did not process any events, so the fraud score and notification evidence is missing.
- Consumer lag for the fraud and notification groups is unmeasured.
- The `payment_producer` run was not repeated, so there is one successful producer run.
