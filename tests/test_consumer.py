"""
Unit tests for the event consumer, transformer, and dedup modules.
Uses fakeredis for isolated in-memory testing.
"""

import pytest
import json
import fakeredis

from processor.transformer import EventTransformer, TransformationError
from processor.dedup import compute_fingerprint, is_duplicate, clear_fingerprint
from processor.dead_letter import DeadLetterQueue
from processor.consumer import EventConsumer
from processor.config import Settings


# ── Transformer Tests ────────────────────────────────────────────

class TestEventTransformer:
    """Tests for the event transformation pipeline."""

    def setup_method(self):
        self.transformer = EventTransformer()

    def test_normalize_standard_event(self):
        raw = {"type": "userSignup", "payload": {"email": "test@example.com"}}
        result = self.transformer.transform(raw)
        assert result is not None
        assert result["event_type"] == "user_signup"
        assert result["data"] == {"email": "test@example.com"}

    def test_normalize_camel_case(self):
        raw = {"eventType": "OrderCreated", "data": {"id": 1}}
        result = self.transformer.transform(raw)
        assert result["event_type"] == "order_created"

    def test_enrich_adds_fields(self):
        raw = {"event_type": "order_created", "data": {"id": 1}}
        result = self.transformer.transform(raw)
        assert "processed_at" in result
        assert "category" in result
        assert "priority" in result
        assert result["category"] == "commerce"

    def test_filter_heartbeat(self):
        raw = {"event_type": "heartbeat", "data": {}}
        result = self.transformer.transform(raw)
        assert result is None

    def test_filter_ping(self):
        raw = {"type": "ping"}
        result = self.transformer.transform(raw)
        assert result is None

    def test_priority_assignment_high(self):
        raw = {"event_type": "error_critical", "data": "something broke"}
        result = self.transformer.transform(raw)
        assert result["priority"] == "high"

    def test_priority_assignment_low(self):
        raw = {"event_type": "analytics_click", "data": {"page": "/home"}}
        result = self.transformer.transform(raw)
        assert result["priority"] == "low"

    def test_categorize_user_event(self):
        raw = {"event_type": "user_login", "data": {}}
        result = self.transformer.transform(raw)
        assert result["category"] == "user_activity"

    def test_categorize_system_event(self):
        raw = {"event_type": "system_restart", "data": {}}
        result = self.transformer.transform(raw)
        assert result["category"] == "infrastructure"

    def test_metadata_preserved(self):
        raw = {"event_type": "custom_event", "data": {}, "custom_field": "value"}
        result = self.transformer.transform(raw)
        assert result["metadata"]["custom_field"] == "value"


# ── Deduplication Tests ──────────────────────────────────────────

class TestDeduplication:
    """Tests for the deduplication module."""

    def setup_method(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)

    def test_fingerprint_deterministic(self):
        data = {"type": "order", "id": 123}
        fp1 = compute_fingerprint(data)
        fp2 = compute_fingerprint(data)
        assert fp1 == fp2

    def test_fingerprint_ignores_retry_count(self):
        data1 = {"type": "order", "id": 123, "retry_count": 0}
        data2 = {"type": "order", "id": 123, "retry_count": 3}
        assert compute_fingerprint(data1) == compute_fingerprint(data2)

    def test_fingerprint_ignores_timestamp(self):
        data1 = {"type": "order", "id": 123, "timestamp": "2024-01-01"}
        data2 = {"type": "order", "id": 123, "timestamp": "2024-06-15"}
        assert compute_fingerprint(data1) == compute_fingerprint(data2)

    def test_first_message_not_duplicate(self):
        fp = compute_fingerprint({"type": "order", "id": 1})
        assert is_duplicate(self.redis, fp) is False

    def test_second_message_is_duplicate(self):
        fp = compute_fingerprint({"type": "order", "id": 1})
        is_duplicate(self.redis, fp)  # First time
        assert is_duplicate(self.redis, fp) is True  # Duplicate

    def test_different_events_not_duplicate(self):
        fp1 = compute_fingerprint({"type": "order", "id": 1})
        fp2 = compute_fingerprint({"type": "order", "id": 2})
        is_duplicate(self.redis, fp1)
        assert is_duplicate(self.redis, fp2) is False

    def test_clear_fingerprint(self):
        fp = compute_fingerprint({"type": "order", "id": 1})
        is_duplicate(self.redis, fp)
        clear_fingerprint(self.redis, fp)
        assert is_duplicate(self.redis, fp) is False


# ── Dead Letter Queue Tests ──────────────────────────────────────

class TestDeadLetterQueue:
    """Tests for the dead-letter queue."""

    def setup_method(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.dlq = DeadLetterQueue(self.redis, "test-dlq")

    def test_send_to_dlq(self):
        event = {"event_type": "order_created", "data": "test"}
        msg_id = self.dlq.send_to_dlq(
            event_data=event,
            error_message="Processing failed",
            retry_count=3,
        )
        assert msg_id is not None

    def test_dlq_size(self):
        assert self.dlq.dlq_size() == 0
        self.dlq.send_to_dlq({"type": "test"}, "error", 1)
        assert self.dlq.dlq_size() == 1

    def test_list_dlq(self):
        self.dlq.send_to_dlq({"type": "test1"}, "error1", 1)
        self.dlq.send_to_dlq({"type": "test2"}, "error2", 2)
        entries = self.dlq.list_dlq()
        assert len(entries) == 2
        assert entries[0]["error_message"] == "error1"

    def test_replay_event(self):
        self.dlq.send_to_dlq({"event_type": "test"}, "error", 1)
        entries = self.dlq.list_dlq()
        dlq_id = entries[0]["dlq_id"]

        new_id = self.dlq.replay_event(dlq_id, target_stream="events")
        assert new_id is not None
        assert self.dlq.dlq_size() == 0

    def test_replay_nonexistent(self):
        result = self.dlq.replay_event("0-0", target_stream="events")
        assert result is None


# ── Integration Test ─────────────────────────────────────────────

class TestConsumerIntegration:
    """Integration tests with fakeredis."""

    def setup_method(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.settings = Settings(
            redis_url="redis://fake",
            stream_name="test-events",
            consumer_group="test-group",
            consumer_name="test-worker",
            dlq_stream_name="test-dlq",
            max_retries=2,
        )

    def test_consumer_setup_creates_group(self):
        consumer = EventConsumer(redis_client=self.redis, settings=self.settings)
        consumer.setup()
        # If no error, group was created
        groups = self.redis.xinfo_groups(self.settings.stream_name)
        assert len(groups) == 1
        assert groups[0]["name"] == "test-group"

    def test_consumer_processes_event(self):
        consumer = EventConsumer(redis_client=self.redis, settings=self.settings)
        consumer.setup()

        # Publish a test event
        self.redis.xadd(self.settings.stream_name, {
            "event_type": "order_created",
            "data": json.dumps({"order_id": "123"}),
        })

        # Process one batch
        consumer._consume_batch()

        # Check output stream
        processed = self.redis.xrange(f"{self.settings.stream_name}-processed")
        assert len(processed) >= 1

    def test_consumer_deduplicates(self):
        consumer = EventConsumer(redis_client=self.redis, settings=self.settings)
        consumer.setup()

        event = {
            "event_type": "order_created",
            "data": json.dumps({"order_id": "123"}),
        }

        # Publish same event twice
        self.redis.xadd(self.settings.stream_name, event)
        self.redis.xadd(self.settings.stream_name, event)

        # Process both
        consumer._consume_batch()

        # Only one should appear in processed stream
        processed = self.redis.xrange(f"{self.settings.stream_name}-processed")
        assert len(processed) == 1

    def test_consumer_filters_heartbeat(self):
        consumer = EventConsumer(redis_client=self.redis, settings=self.settings)
        consumer.setup()

        self.redis.xadd(self.settings.stream_name, {
            "event_type": "heartbeat",
            "data": "{}",
        })

        consumer._consume_batch()

        processed = self.redis.xrange(f"{self.settings.stream_name}-processed")
        assert len(processed) == 0
