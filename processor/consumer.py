"""
Redis Streams consumer with consumer groups for at-least-once delivery.

Key features:
- Consumer group for parallel processing across workers
- Automatic ACK after successful processing
- Retry tracking with exponential backoff
- Deduplication via content fingerprinting
- Dead-letter queue for permanently failed messages
- Prometheus metrics for observability
"""

import time
import json
import signal
import sys
from typing import Optional

import redis
import structlog

from processor.config import get_settings
from processor.dedup import compute_fingerprint, is_duplicate
from processor.transformer import EventTransformer, TransformationError
from processor.dead_letter import DeadLetterQueue
from processor.metrics import (
    events_processed_total,
    events_errors_total,
    events_deduplicated_total,
    processing_latency_seconds,
    backlog_size,
    active_workers,
)

logger = structlog.get_logger()

# Configure structlog
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)


class EventConsumer:
    """
    Redis Streams consumer that:
    1. Reads events from a stream using consumer groups
    2. Deduplicates based on content fingerprint
    3. Transforms events through a pipeline
    4. Writes results to an output stream
    5. ACKs messages only after successful processing
    6. Retries failures with exponential backoff
    7. Dead-letters permanently failed messages
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        settings=None,
    ):
        self.settings = settings or get_settings()
        self.redis = redis_client or redis.from_url(
            self.settings.redis_url, decode_responses=True
        )
        self.transformer = EventTransformer()
        self.dlq = DeadLetterQueue(self.redis, self.settings.dlq_stream_name)
        self.running = False

        # Retry tracking: message_id -> retry_count
        self._retry_counts: dict[str, int] = {}

    def setup(self):
        """Initialize the consumer group if it doesn't exist."""
        try:
            self.redis.xgroup_create(
                self.settings.stream_name,
                self.settings.consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "consumer_group_created",
                stream=self.settings.stream_name,
                group=self.settings.consumer_group,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("consumer_group_exists", group=self.settings.consumer_group)
            else:
                raise

    def start(self):
        """Start consuming events. Blocks until stopped."""
        self.setup()
        self.running = True
        active_workers.inc()

        # Handle graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        logger.info(
            "consumer_started",
            consumer=self.settings.consumer_name,
            group=self.settings.consumer_group,
            stream=self.settings.stream_name,
        )

        try:
            # First, reclaim any pending messages from previous crashes
            self._process_pending()

            # Then consume new messages
            while self.running:
                self._consume_batch()
                self._update_backlog_metric()
        finally:
            active_workers.dec()
            logger.info("consumer_stopped", consumer=self.settings.consumer_name)

    def _consume_batch(self):
        """Read and process a batch of messages from the stream."""
        try:
            messages = self.redis.xreadgroup(
                groupname=self.settings.consumer_group,
                consumername=self.settings.consumer_name,
                streams={self.settings.stream_name: ">"},
                count=self.settings.batch_size,
                block=self.settings.block_ms,
            )
        except redis.ConnectionError:
            logger.error("redis_connection_lost", action="reconnecting")
            time.sleep(2)
            return

        if not messages:
            return

        for stream_name, stream_messages in messages:
            for msg_id, fields in stream_messages:
                self._process_message(msg_id, fields)

    def _process_pending(self):
        """Reclaim and reprocess pending messages from previous runs."""
        logger.info("processing_pending_messages")

        try:
            pending = self.redis.xpending_range(
                self.settings.stream_name,
                self.settings.consumer_group,
                min="-",
                max="+",
                count=100,
                consumername=self.settings.consumer_name,
            )
        except redis.ResponseError:
            return

        for entry in pending:
            msg_id = entry["message_id"]
            messages = self.redis.xrange(
                self.settings.stream_name, min=msg_id, max=msg_id, count=1
            )
            if messages:
                _, fields = messages[0]
                self._process_message(msg_id, fields)

    def _process_message(self, msg_id: str, fields: dict):
        """Process a single message with dedup, transform, and error handling."""
        start_time = time.time()
        event_type = fields.get("event_type", fields.get("type", "unknown"))

        structlog.contextvars.bind_contextvars(
            msg_id=msg_id,
            event_type=event_type,
        )

        try:
            # Step 1: Deduplication
            fingerprint = compute_fingerprint(fields)
            if is_duplicate(self.redis, fingerprint):
                events_deduplicated_total.inc()
                self._ack(msg_id)
                return

            # Step 2: Transform
            result = self.transformer.transform(fields)

            if result is None:
                # Event was filtered out
                self._ack(msg_id)
                return

            # Step 3: Write result to output stream
            output_data = {
                k: str(v) if not isinstance(v, str) else v
                for k, v in result.items()
            }
            self.redis.xadd(f"{self.settings.stream_name}-processed", output_data)

            # Step 4: ACK the message
            self._ack(msg_id)

            # Step 5: Record metrics
            latency = time.time() - start_time
            processing_latency_seconds.labels(event_type=event_type).observe(latency)
            events_processed_total.labels(
                event_type=event_type, status="success"
            ).inc()

            logger.info(
                "event_processed",
                latency_ms=round(latency * 1000, 2),
            )

        except TransformationError as e:
            self._handle_failure(msg_id, fields, str(e), event_type, "transformation_error")
        except Exception as e:
            self._handle_failure(msg_id, fields, str(e), event_type, "processing_error")

    def _handle_failure(
        self,
        msg_id: str,
        fields: dict,
        error: str,
        event_type: str,
        error_type: str,
    ):
        """Handle processing failures with retry or dead-letter."""
        retry_count = self._retry_counts.get(msg_id, 0) + 1
        self._retry_counts[msg_id] = retry_count

        events_errors_total.labels(
            event_type=event_type, error_type=error_type
        ).inc()

        if retry_count >= self.settings.max_retries:
            # Max retries exceeded — send to DLQ
            logger.error(
                "max_retries_exceeded",
                retry_count=retry_count,
                error=error,
            )
            self.dlq.send_to_dlq(
                event_data=fields,
                error_message=error,
                retry_count=retry_count,
                original_stream=self.settings.stream_name,
                original_id=msg_id,
            )
            self._ack(msg_id)
            self._retry_counts.pop(msg_id, None)
        else:
            # Exponential backoff before retry
            backoff = min(2 ** retry_count, 30)
            logger.warning(
                "event_processing_retry",
                retry_count=retry_count,
                backoff_seconds=backoff,
                error=error,
            )
            time.sleep(backoff)

    def _ack(self, msg_id: str):
        """Acknowledge a message in the consumer group."""
        self.redis.xack(
            self.settings.stream_name,
            self.settings.consumer_group,
            msg_id,
        )

    def _update_backlog_metric(self):
        """Update the backlog gauge with current stream length."""
        try:
            stream_len = self.redis.xlen(self.settings.stream_name)
            backlog_size.set(stream_len)
        except redis.ConnectionError:
            pass

    def _handle_shutdown(self, signum, frame):
        """Graceful shutdown handler."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False

    def stop(self):
        """Stop the consumer loop."""
        self.running = False
