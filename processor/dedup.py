"""
Deduplication module using Redis SET-based fingerprints.

Each event is fingerprinted using its content hash. If the fingerprint
already exists in Redis, the event is skipped (deduplication).
Fingerprints expire after a configurable TTL to prevent unbounded growth.
"""

import hashlib
import json
from typing import Optional

import redis
import structlog

logger = structlog.get_logger()

# Default TTL for dedup fingerprints: 24 hours
DEFAULT_DEDUP_TTL = 86400
DEDUP_KEY_PREFIX = "dedup:"


def compute_fingerprint(event_data: dict) -> str:
    """
    Compute a deterministic fingerprint (SHA-256) for an event.
    Uses sorted JSON serialization for consistency.
    """
    # Remove metadata fields that differ between retries
    data = {k: v for k, v in event_data.items() if k not in ("retry_count", "timestamp")}
    canonical = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def is_duplicate(
    redis_client: redis.Redis,
    fingerprint: str,
    ttl: int = DEFAULT_DEDUP_TTL,
) -> bool:
    """
    Check if an event fingerprint has been seen before.
    If not, marks it as seen with a TTL.

    Returns True if the event is a duplicate, False otherwise.
    """
    key = f"{DEDUP_KEY_PREFIX}{fingerprint}"

    # SETNX + TTL in a single atomic operation
    was_set = redis_client.set(key, "1", nx=True, ex=ttl)

    if was_set:
        # First time seeing this fingerprint
        return False
    else:
        logger.info("duplicate_detected", fingerprint=fingerprint[:16])
        return True


def clear_fingerprint(redis_client: redis.Redis, fingerprint: str) -> None:
    """Remove a fingerprint from the dedup store (for manual retries)."""
    key = f"{DEDUP_KEY_PREFIX}{fingerprint}"
    redis_client.delete(key)
