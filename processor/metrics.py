"""
Prometheus metrics instrumentation for the event processor.

Tracks:
- Processing latency (histogram)
- Events processed (counter) — total throughput
- Processing errors (counter)
- Backlog size (gauge) — pending messages in the stream
"""

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    start_http_server,
    CollectorRegistry,
)

# Use a custom registry so tests don't conflict with global state
registry = CollectorRegistry()

# ── Counters ─────────────────────────────────────────────────────

events_processed_total = Counter(
    "events_processed_total",
    "Total number of events successfully processed",
    ["event_type", "status"],
    registry=registry,
)

events_errors_total = Counter(
    "events_errors_total",
    "Total number of processing errors",
    ["event_type", "error_type"],
    registry=registry,
)

events_deduplicated_total = Counter(
    "events_deduplicated_total",
    "Total number of duplicate events skipped",
    registry=registry,
)

events_dead_lettered_total = Counter(
    "events_dead_lettered_total",
    "Total number of events sent to dead-letter queue",
    ["event_type"],
    registry=registry,
)

# ── Histogram ────────────────────────────────────────────────────

processing_latency_seconds = Histogram(
    "event_processing_latency_seconds",
    "Time spent processing each event",
    ["event_type"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
    registry=registry,
)

# ── Gauges ───────────────────────────────────────────────────────

backlog_size = Gauge(
    "event_backlog_size",
    "Number of pending messages in the stream (not yet consumed)",
    registry=registry,
)

active_workers = Gauge(
    "active_workers",
    "Number of currently active worker instances",
    registry=registry,
)


def start_metrics_server(port: int = 9100):
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port, registry=registry)
