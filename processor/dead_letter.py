"""
Dead-letter queue (DLQ) handler.

Events that fail processing after all retries are moved to a
dead-letter stream for manual inspection and replay.
"""

import json
from datetime import datetime, timezone
from typing import Optional

import redis
import structlog

from processor.metrics import events_dead_lettered_total

logger = structlog.get_logger()


class DeadLetterQueue:
    """
    Manages a dead-letter stream in Redis for failed events.
    Provides methods to send to DLQ, list DLQ contents, and replay events.
    """

    def __init__(self, redis_client: redis.Redis, dlq_stream: str = "events-dlq"):
        self.redis = redis_client
        self.dlq_stream = dlq_stream

    def send_to_dlq(
        self,
        event_data: dict,
        error_message: str,
        retry_count: int,
        original_stream: str = "events",
        original_id: str = "",
    ) -> str:
        """
        Send a failed event to the dead-letter queue with failure metadata.
        Returns the DLQ message ID.
        """
        dlq_entry = {
            "original_stream": original_stream,
            "original_id": original_id,
            "event_data": json.dumps(event_data, default=str),
            "error_message": error_message,
            "retry_count": str(retry_count),
            "dead_lettered_at": datetime.now(timezone.utc).isoformat(),
        }

        msg_id = self.redis.xadd(self.dlq_stream, dlq_entry)

        event_type = event_data.get("event_type", event_data.get("type", "unknown"))
        events_dead_lettered_total.labels(event_type=event_type).inc()

        logger.warning(
            "event_dead_lettered",
            dlq_id=msg_id,
            original_id=original_id,
            error=error_message,
            retry_count=retry_count,
        )

        return msg_id

    def list_dlq(self, count: int = 100) -> list[dict]:
        """List recent entries in the DLQ."""
        messages = self.redis.xrange(self.dlq_stream, count=count)
        results = []
        for msg_id, fields in messages:
            entry = {k: v for k, v in fields.items()}
            entry["dlq_id"] = msg_id
            if "event_data" in entry:
                try:
                    entry["event_data"] = json.loads(entry["event_data"])
                except (json.JSONDecodeError, TypeError):
                    pass
            results.append(entry)
        return results

    def dlq_size(self) -> int:
        """Get the current number of messages in the DLQ."""
        return self.redis.xlen(self.dlq_stream)

    def replay_event(
        self,
        dlq_id: str,
        target_stream: str = "events",
    ) -> Optional[str]:
        """
        Replay a DLQ event by re-publishing it to the target stream.
        Returns the new message ID or None if the DLQ entry was not found.
        """
        messages = self.redis.xrange(self.dlq_stream, min=dlq_id, max=dlq_id, count=1)
        if not messages:
            logger.warning("dlq_entry_not_found", dlq_id=dlq_id)
            return None

        _, fields = messages[0]
        event_data_str = fields.get("event_data", "{}")
        try:
            event_data = json.loads(event_data_str)
        except (json.JSONDecodeError, TypeError):
            event_data = {"raw": event_data_str}

        # Re-publish to target stream
        flat_data = {k: str(v) if not isinstance(v, str) else v for k, v in event_data.items()}
        new_id = self.redis.xadd(target_stream, flat_data)

        # Remove from DLQ
        self.redis.xdel(self.dlq_stream, dlq_id)

        logger.info(
            "dlq_event_replayed",
            dlq_id=dlq_id,
            new_id=new_id,
            target_stream=target_stream,
        )
        return new_id
