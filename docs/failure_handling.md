# Failure Handling Policy

This document defines how the Distributed Event Processor handles failures at each stage of processing.

---

## Failure Categories

### 1. Transient Failures (Retryable)

**Examples**: Network timeouts, temporary Redis unavailability, rate limits

**Policy**:
- Retry up to `MAX_RETRIES` times (default: 3)
- Use **exponential backoff**: 2s → 4s → 8s (capped at 30s)
- Message stays in the pending list during retries
- Each retry is counted and logged

```
Attempt 1: Immediate
Attempt 2: Wait 2s
Attempt 3: Wait 4s
→ If all fail: Send to Dead-Letter Queue
```

### 2. Permanent Failures (Non-Retryable)

**Examples**: Malformed event data, transformation errors, schema violations

**Policy**:
- Skip retry and immediately send to Dead-Letter Queue (DLQ)
- Log the error with full context (event data, error message, stack trace)
- Increment `events_dead_lettered_total` metric

### 3. Infrastructure Failures

**Examples**: Redis connection lost, out of memory

**Policy**:
- Consumer enters a reconnection loop with 2-second intervals
- All in-flight messages remain in the pending list (no data loss)
- Prometheus `active_workers` gauge drops, triggering `NoActiveWorkers` alert
- On reconnection, pending messages are reclaimed and reprocessed

---

## Dead-Letter Queue (DLQ) Procedures

### When Events Go to the DLQ

1. Event fails processing **3 consecutive times** (configurable via `MAX_RETRIES`)
2. Event has a **permanent transformation error**
3. Manual operator decision

### DLQ Monitoring

```bash
# Check DLQ size
python -m processor.main dlq-list --count 20

# View recent DLQ entries
python -m processor.main dlq-list
```

### DLQ Replay

After fixing the root cause:

```bash
# Replay a specific event
python -m processor.main dlq-replay <dlq_message_id>
```

### DLQ Escalation

| DLQ Growth Rate | Action |
|-----------------|--------|
| < 5 events/hour | Monitor, investigate during business hours |
| 5–20 events/hour | Investigate immediately, check for systematic issues |
| > 20 events/hour | **Incident**: Stop the pipeline, fix root cause first |

---

## At-Least-Once Delivery Guarantees

### How It Works

1. Consumer reads messages using `XREADGROUP` → message is assigned to the consumer
2. Processing happens (dedup → transform → write output)
3. Only after successful processing does the consumer call `XACK`
4. If the consumer crashes before ACK, the message remains pending
5. On restart, `XPENDING` reclaims unacknowledged messages

### Implications

- Events may be processed **more than once** if the consumer crashes after processing but before ACK
- The **deduplication layer** prevents duplicate output writes
- Consumers must be **idempotent** — processing the same event twice should produce the same result

---

## Monitoring Checklist

| Metric | Alert Threshold | Dashboard |
|--------|----------------|-----------|
| `event_backlog_size` | > 1000 for 5min | Prometheus |
| `events_errors_total` rate | > 0.1/sec for 3min | Prometheus |
| `events_dead_lettered_total` | > 10/hour | Prometheus |
| `active_workers` | == 0 for 1min | Prometheus |
| `event_processing_latency_seconds` P95 | > 5s for 5min | Prometheus |

---

## Escalation Contacts

| Severity | Response Time | Contact |
|----------|---------------|---------|
| Critical | 15 minutes | On-call engineer (PagerDuty) |
| Warning | 1 hour | Team Slack channel |
| Info | Next business day | JIRA ticket |
