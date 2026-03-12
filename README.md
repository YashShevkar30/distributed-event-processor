# Distributed Event Processor

An event-driven worker that consumes messages from Redis Streams, performs transformations, and writes results with **at-least-once semantics**, **deduplication protection**, and full **observability instrumentation**.

Built with **Python**, **Redis Streams**, **Prometheus**, and containerized with **Docker**.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Streams-DC382D?logo=redis&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?logo=prometheus&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?logo=github-actions&logoColor=white)

---

## Features

### Event Processing
- **Consumer Groups** — Parallel processing across multiple workers using Redis consumer groups
- **At-Least-Once Delivery** — Messages are only ACKed after successful processing
- **Pending Message Recovery** — On restart, reclaims and reprocesses unacknowledged messages

### Transformation Pipeline
- **Normalization** — Standardize field names (camelCase → snake_case) and formats
- **Enrichment** — Add processing timestamps, categories, priority levels
- **Filtering** — Skip heartbeat/ping events automatically
- **Validation** — Ensure transformed events have required fields

### Deduplication
- **Content Fingerprinting** — SHA-256 hash of event content (ignoring retry metadata)
- **Redis-Backed** — Fingerprint store with configurable TTL (default: 24h)
- **Zero Duplicates in Output** — Even under at-least-once delivery

### Failure Handling
- **Exponential Backoff** — Retries with 2s → 4s → 8s delays (capped at 30s)
- **Dead-Letter Queue** — Permanently failed messages are moved to a DLQ stream
- **DLQ Management** — CLI commands to list, inspect, and replay DLQ events

### Observability
- **Prometheus Metrics** — Processing latency, throughput, error rates, backlog size
- **Alerting Rules** — Pre-configured alerts for backlog growth, error spikes, and worker health
- **Structured Logging** — JSON-formatted logs with message context

```
┌────────────┐     ┌─────────────────────────────────────────┐     ┌──────────────┐
│  Producer   │────▶│          Redis Stream (events)           │────▶│   Consumer    │
└────────────┘     └─────────────────────────────────────────┘     │   Worker(s)   │
                                                                    │               │
                                                                    │  ┌──────────┐ │
                                                                    │  │  Dedup   │ │
                                                                    │  └────┬─────┘ │
                                                                    │       ▼       │
                                                                    │  ┌──────────┐ │
                                                                    │  │Transform │ │
                                                                    │  └────┬─────┘ │
                                                                    │       ▼       │
                                                                    │  ┌──────────┐ │     ┌─────────────┐
                                                                    │  │  Write   │─┼────▶│  Processed  │
                                                                    │  └────┬─────┘ │     │   Stream    │
                                                                    │       │       │     └─────────────┘
                                                                    │       ▼       │
                                                                    │  ┌──────────┐ │     ┌─────────────┐
                                                                    │  │  ACK /   │─┼────▶│  Dead-Letter │
                                                                    │  │  Retry   │ │     │   Queue      │
                                                                    │  └──────────┘ │     └─────────────┘
                                                                    └───────┬───────┘
                                                                            │
                                                                            ▼
                                                                    ┌──────────────┐
                                                                    │  Prometheus  │
                                                                    │   Metrics    │
                                                                    └──────────────┘
```

---

## Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/YashShevkar30/distributed-event-processor.git
cd distributed-event-processor

# Start the full stack (Redis + 2 Workers + Prometheus)
docker-compose up --build -d

# Verify services are running
docker-compose ps

# Produce test events
pip install redis click
python -m producer.producer --count 50 --burst
```

- **Prometheus Dashboard**: http://localhost:9090
- **Worker 1 Metrics**: http://localhost:9100/metrics
- **Worker 2 Metrics**: http://localhost:9101/metrics

### Option 2: Local Development

```bash
# Clone and set up
git clone https://github.com/YashShevkar30/distributed-event-processor.git
cd distributed-event-processor

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Redis (requires Docker)
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Set up environment
cp .env.example .env

# Start the worker
python -m processor.main start

# In another terminal, produce test events
python -m producer.producer --count 20
```

---

## CLI Reference

### Worker Commands

```bash
# Start the event processor
python -m processor.main start

# Start with custom consumer name
python -m processor.main start --consumer-name worker-3

# Start without metrics server
python -m processor.main start --no-metrics
```

### DLQ Commands

```bash
# List dead-letter queue entries
python -m processor.main dlq-list --count 20

# Replay a specific DLQ event
python -m processor.main dlq-replay <message_id>
```

### Producer Commands

```bash
# Produce 10 events with 0.5s delay
python -m producer.producer --count 10

# Burst mode (no delay)
python -m producer.producer --count 100 --burst

# Custom Redis URL
python -m producer.producer --redis-url redis://myhost:6379/0
```

---

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `events_processed_total` | Counter | Total events successfully processed |
| `events_errors_total` | Counter | Total processing errors by type |
| `events_deduplicated_total` | Counter | Duplicate events skipped |
| `events_dead_lettered_total` | Counter | Events sent to DLQ |
| `event_processing_latency_seconds` | Histogram | Per-event processing time |
| `event_backlog_size` | Gauge | Pending messages in the stream |
| `active_workers` | Gauge | Currently running workers |

---

## Alert Rules

Pre-configured Prometheus alerts (see [alert_rules.yml](alert_rules.yml)):

| Alert | Condition | Severity |
|-------|-----------|----------|
| `EventBacklogHigh` | Backlog > 1000 for 5min | Warning |
| `EventBacklogCritical` | Backlog > 5000 for 2min | Critical |
| `HighErrorRate` | Error rate > 0.1/sec for 3min | Warning |
| `DLQGrowing` | > 10 dead-lettered events/hour | Warning |
| `NoActiveWorkers` | 0 active workers for 1min | Critical |
| `HighProcessingLatency` | P95 latency > 5s for 5min | Warning |

---

## Project Structure

```
distributed-event-processor/
├── processor/
│   ├── __init__.py
│   ├── main.py              # CLI entry point
│   ├── config.py             # Configuration management
│   ├── consumer.py           # Redis Streams consumer
│   ├── transformer.py        # Event transformation pipeline
│   ├── dedup.py              # Deduplication via fingerprinting
│   ├── dead_letter.py        # Dead-letter queue handler
│   └── metrics.py            # Prometheus instrumentation
├── producer/
│   ├── __init__.py
│   └── producer.py           # Test event producer
├── tests/
│   ├── __init__.py
│   └── test_consumer.py      # Unit + integration tests
├── docs/
│   └── failure_handling.md   # Failure handling policy
├── .github/
│   └── workflows/
│       └── ci.yml            # CI pipeline
├── Dockerfile
├── docker-compose.yml        # Full stack (Redis + Workers + Prometheus)
├── prometheus.yml            # Prometheus scrape config
├── alert_rules.yml           # Alerting rules
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=processor --cov-report=term-missing
```

Tests use **fakeredis** for fast, isolated execution — no Redis instance required.

---

## Failure Handling Policy

See [docs/failure_handling.md](docs/failure_handling.md) for the complete failure handling policy including:
- Retry strategy and backoff configuration
- Dead-letter queue procedures
- At-least-once delivery guarantees
- Escalation matrix

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Language** | Python 3.11 |
| **Message Broker** | Redis Streams 7.x |
| **Monitoring** | Prometheus + Alert Rules |
| **Logging** | structlog (JSON) |
| **Testing** | pytest + fakeredis |
| **Container** | Docker + Docker Compose |
| **CI/CD** | GitHub Actions |

---

## License

This project is open source and available under the [MIT License](LICENSE).

## Architecture Notes
Additional architecture documentation to be added here.


## Contributing
Contributions are welcome! Please open an issue first.
