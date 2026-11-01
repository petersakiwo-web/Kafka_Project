# Test 2 Summary: Kafka Payment Events POC (after fixes)

**Environment:** Rancher Desktop Kubernetes, 3 Kafka brokers, host access via NodePorts `localhost:30092-30094`.
**Test size:** 60 payment journeys, 235 events on `digital.payments.lifecycle.v1`.

## Results

| Check | Result | Evidence |
|---|---|---|
| Topics created | PASS | `topic_describe_payments.txt` (6 partitions, RF 3, minISR 2, zstd, delete, 7-year retention) |
| Producer delivered every event | PASS | `producer.log`: 235 broker acks, 0 DLQ entries |
| Fraud consumer reads and scores | PASS | `counts.txt`: 60 scored (initiated events) |
| Notification consumer reads and sends | PASS | `counts.txt`: 63 sent (55 approved + 8 failed) |
| Audit consumer archives every event | PASS | `audit_archive_check.txt`: 235 unique events, 0 duplicates |
| Lag is zero for each group | PASS | `lag_per_partition.txt`: all 6 partitions, all 3 groups, lag 0 |
| Event ordering per payment | PASS | `ordering_check.txt`: 60 payments, 235 events, one partition each, offsets and `sequence_no` in order |

Per-partition committed offsets (identical for each group): 27 + 40 + 27 + 46 + 36 + 59 = 235.

## Issues found and fixed (test 2)

### A. Brokers OOM-killed (root cause of test 1 failures)
- **Found:** `kubectl get pod` showed `OOMKilled` on `kafka-0` and `kafka-1`. The brokers run `-Xmx1G` in a 1 GiB container, and they were restarting every few minutes. Coordinator loads kept failing, so the fraud and notification groups never joined.
- **Fix:** Reduced the POC topic sizes. The topics were deleted and recreated with 6 / 6 / 3 partitions instead of 100 / 100 / 50. `create-topics.sh` now takes `PAYMENT_PARTITIONS`, `FRAUD_PARTITIONS` and `NOTIFICATION_PARTITIONS` as overrides, and the defaults are still the production values.
- **Not changed:** The broker heap and memory limits, which belong to the shared cluster. Raising them is the other option if production-sized partitions are needed here.

### B. Offsets committed for one partition only (consumer bug)
- **Found:** The notification group showed a committed offset on partition 5 only. The other five partitions had no commit. The bug was `consumer.commit(message=last_msg)`, which commits only the partition of the last message. The audit consumer had the same bug, so restarts re-read records.
- **Fix:** `common.py` now tracks an offset per (topic, partition) with `track_offset`, and `commit_offsets` commits all of them. All three consumers use it.

### C. No output and no commits for short runs
- **Found:** Consumers committed only every 100 messages. A 60-journey run never reached 100, so nothing was committed. Output was also block-buffered when redirected to a file, so the logs looked empty.
- **Fix:** Each consumer commits at least every 2 seconds (`COMMIT_INTERVAL_S`). Print statements use `flush=True`.

### D. Stopping consumers with SIGINT on Windows (carried over from test 1)
- **Status:** Still a limitation. The consumers were left to exit on their own `timeout`. The timed commits in fix C now limit how much work can be lost, but the `finally` block still does not run on a hard kill.

### E. Shell lag check was too strict
- **Found:** A `kubectl exec kafka-consumer-groups --describe` loop reported 3 partitions as lagging or unassigned. Those `kubectl` calls were slow (about 40 seconds each) and timed out at times.
- **Resolution:** Lag was measured directly with the Kafka admin API in `lag_check.py`, which compares committed offsets with the partition end offsets. It shows 0 lag on all partitions, so the result above is based on that measurement.

### F. Test state reset
- The three test consumer groups and the three test topics were deleted before test 2, because they held test-1 data created by this project. Other topics on the cluster (`digitalpayments.*`, `__consumer_offsets`) were not changed.

## Known limitations
- The POC uses 6 / 6 / 3 partitions. The design target (100 / 100 / 50) needs the brokers to be sized for it.
- A consumer killed with a hard stop may reprocess up to 2 seconds of messages. Fraud output is duplicated in that case, and the `source_event_id` lets downstream code remove the duplicates.
- Fraud velocity state is in memory, so restarts reset it.
