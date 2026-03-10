"""
Event transformation pipeline.

Takes raw event data and applies a series of transformations:
1. Normalization — standardize field names and formats
2. Enrichment — add computed fields (timestamps, categories)
3. Filtering — skip events that don't meet criteria
4. Validation — ensure transformed data is well-formed
"""

import re
from datetime import datetime, timezone
from typing import Optional

import structlog

logger = structlog.get_logger()


class TransformationError(Exception):
    """Raised when an event fails transformation."""
    pass


class EventTransformer:
    """Pipeline for transforming raw events into processed events."""

    def transform(self, raw_event: dict) -> Optional[dict]:
        """
        Run the full transformation pipeline.

        Returns the transformed event, or None if the event should be filtered out.
        Raises TransformationError for unrecoverable issues.
        """
        try:
            event = self._normalize(raw_event)

            if not self._should_process(event):
                logger.info("event_filtered", event_type=event.get("event_type"))
                return None

            event = self._enrich(event)
            self._validate(event)

            return event

        except TransformationError:
            raise
        except Exception as e:
            raise TransformationError(f"Transformation failed: {str(e)}")

    def _normalize(self, raw: dict) -> dict:
        """Normalize field names and formats."""
        event = {}

        # Standardize common field names
        field_mapping = {
            "type": "event_type",
            "event_type": "event_type",
            "eventType": "event_type",
            "payload": "data",
            "data": "data",
            "body": "data",
            "ts": "source_timestamp",
            "timestamp": "source_timestamp",
            "created_at": "source_timestamp",
            "source": "source",
            "origin": "source",
            "id": "event_id",
            "event_id": "event_id",
            "eventId": "event_id",
        }

        for raw_key, normalized_key in field_mapping.items():
            if raw_key in raw:
                event[normalized_key] = raw[raw_key]

        # Ensure event_type is lowercase and snake_case
        if "event_type" in event and isinstance(event["event_type"], str):
            event["event_type"] = self._to_snake_case(event["event_type"])

        # Carry over any unmapped fields as metadata
        mapped_keys = set(field_mapping.keys())
        extra = {k: v for k, v in raw.items() if k not in mapped_keys}
        if extra:
            event["metadata"] = extra

        return event

    def _should_process(self, event: dict) -> bool:
        """Filter out events that should not be processed."""
        event_type = event.get("event_type", "")

        # Skip heartbeat/ping events
        skip_types = {"heartbeat", "ping", "health_check", "test_event"}
        if event_type in skip_types:
            return False

        # Skip events without meaningful data
        if not event.get("data") and not event.get("event_type"):
            return False

        return True

    def _enrich(self, event: dict) -> dict:
        """Add computed fields to the event."""
        # Add processing timestamp
        event["processed_at"] = datetime.now(timezone.utc).isoformat()

        # Categorize the event
        event["category"] = self._categorize(event.get("event_type", "unknown"))

        # Compute data size if data is present
        if "data" in event and isinstance(event["data"], (dict, str)):
            event["data_size_bytes"] = len(str(event["data"]))

        # Add priority based on event type
        event["priority"] = self._assign_priority(event.get("event_type", ""))

        return event

    def _validate(self, event: dict) -> None:
        """Validate the transformed event has required fields."""
        required_fields = ["event_type", "processed_at"]
        missing = [f for f in required_fields if f not in event]
        if missing:
            raise TransformationError(
                f"Transformed event missing required fields: {missing}"
            )

    def _categorize(self, event_type: str) -> str:
        """Assign a category based on event type prefix."""
        categories = {
            "user": "user_activity",
            "order": "commerce",
            "payment": "commerce",
            "system": "infrastructure",
            "alert": "monitoring",
            "error": "monitoring",
            "log": "observability",
        }
        for prefix, category in categories.items():
            if event_type.startswith(prefix):
                return category
        return "general"

    def _assign_priority(self, event_type: str) -> str:
        """Assign processing priority."""
        high_priority = {"error", "alert", "payment_failed", "system_critical"}
        low_priority = {"log", "analytics", "user_pageview"}

        if any(event_type.startswith(p) for p in high_priority):
            return "high"
        if any(event_type.startswith(p) for p in low_priority):
            return "low"
        return "normal"

    @staticmethod
    def _to_snake_case(name: str) -> str:
        """Convert camelCase or PascalCase to snake_case."""
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
        return s.lower().replace("-", "_").replace(" ", "_")
