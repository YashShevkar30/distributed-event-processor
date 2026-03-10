"""
Test event producer for generating sample events into the Redis stream.
Useful for development, testing, and demos.
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone

import click
import redis


EVENT_TYPES = [
    "user_signup",
    "user_login",
    "order_created",
    "order_updated",
    "payment_received",
    "payment_failed",
    "system_alert",
    "error_logged",
    "analytics_pageview",
]

SAMPLE_DATA = {
    "user_signup": {"user_id": "usr_123", "email": "test@example.com", "plan": "pro"},
    "user_login": {"user_id": "usr_123", "ip": "192.168.1.1", "method": "oauth"},
    "order_created": {"order_id": "ord_456", "amount": 99.99, "items": 3},
    "order_updated": {"order_id": "ord_456", "status": "shipped"},
    "payment_received": {"payment_id": "pay_789", "amount": 99.99, "currency": "USD"},
    "payment_failed": {"payment_id": "pay_790", "error": "insufficient_funds"},
    "system_alert": {"severity": "warning", "message": "CPU usage above 80%"},
    "error_logged": {"level": "error", "service": "auth", "message": "Token expired"},
    "analytics_pageview": {"page": "/dashboard", "session_id": "sess_abc"},
}


@click.command()
@click.option("--redis-url", default="redis://localhost:6379/0", help="Redis URL")
@click.option("--stream", default="events", help="Target stream name")
@click.option("--count", default=10, help="Number of events to produce")
@click.option("--delay", default=0.5, type=float, help="Delay between events (seconds)")
@click.option("--burst", is_flag=True, help="Send all events at once (no delay)")
def produce(redis_url: str, stream: str, count: int, delay: float, burst: bool):
    """Produce sample events to the Redis stream."""
    r = redis.from_url(redis_url, decode_responses=True)

    click.echo(f"Producing {count} events to stream '{stream}'...")

    for i in range(count):
        event_type = random.choice(EVENT_TYPES)
        data = SAMPLE_DATA.get(event_type, {}).copy()

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test-producer",
            "data": json.dumps(data),
        }

        msg_id = r.xadd(stream, event)
        click.echo(f"  [{i+1}/{count}] {event_type} → {msg_id}")

        if not burst:
            time.sleep(delay)

    click.echo(f"Done! Stream '{stream}' length: {r.xlen(stream)}")


if __name__ == "__main__":
    produce()
