"""Unit tests for Consistent Hash Ring and Distributed Queue."""
import pytest
from src.nodes.queue_node import ConsistentHashRing, Message
import time


class TestConsistentHashRing:
    def test_empty_ring_returns_none(self):
        ring = ConsistentHashRing()
        assert ring.get_node("any-key") is None

    def test_single_node_handles_all_keys(self):
        ring = ConsistentHashRing(vnodes=10)
        ring.add_node("http://node1:8001")
        for key in ["key1", "key2", "key3", "abc", "xyz"]:
            assert ring.get_node(key) == "http://node1:8001"

    def test_two_nodes_distribute_keys(self):
        ring = ConsistentHashRing(vnodes=100)
        ring.add_node("http://node1:8001")
        ring.add_node("http://node2:8002")

        keys = [f"key-{i}" for i in range(100)]
        assignments = [ring.get_node(k) for k in keys]

        node1_count = assignments.count("http://node1:8001")
        node2_count = assignments.count("http://node2:8002")

        # With 100 vnodes each, distribution should be roughly 50/50
        assert 20 <= node1_count <= 80, f"Imbalanced: node1={node1_count}, node2={node2_count}"
        assert node1_count + node2_count == 100

    def test_same_key_same_node(self):
        """Same key should always map to same node (deterministic)."""
        ring = ConsistentHashRing(vnodes=50)
        ring.add_node("http://node1:8001")
        ring.add_node("http://node2:8002")
        ring.add_node("http://node3:8003")

        for key in ["queue-orders", "queue-payments", "queue-events"]:
            first = ring.get_node(key)
            for _ in range(10):
                assert ring.get_node(key) == first

    def test_remove_node_remaps_minimally(self):
        """After removing a node, only its keys should remap."""
        ring = ConsistentHashRing(vnodes=100)
        ring.add_node("http://node1:8001")
        ring.add_node("http://node2:8002")
        ring.add_node("http://node3:8003")

        keys = [f"key-{i}" for i in range(200)]
        before = {k: ring.get_node(k) for k in keys}

        ring.remove_node("http://node2:8002")
        after = {k: ring.get_node(k) for k in keys}

        # Keys that were on node2 should remap; others should stay
        remapped = sum(1 for k in keys if before[k] != after[k])
        # Roughly 1/3 of keys should remap
        assert remapped < 150, f"Too many keys remapped: {remapped}"

    def test_get_nodes_for_replication(self):
        """Should return multiple distinct nodes for replication."""
        ring = ConsistentHashRing(vnodes=50)
        ring.add_node("http://node1:8001")
        ring.add_node("http://node2:8002")
        ring.add_node("http://node3:8003")

        nodes = ring.get_nodes_for_key("replicated-queue", n=3)
        assert len(nodes) == 3
        assert len(set(nodes)) == 3  # All distinct

    def test_get_nodes_for_replication_limited_by_available(self):
        """Should not return more nodes than available."""
        ring = ConsistentHashRing(vnodes=50)
        ring.add_node("http://node1:8001")
        ring.add_node("http://node2:8002")

        nodes = ring.get_nodes_for_key("any-key", n=5)
        assert len(nodes) == 2  # Only 2 available

    def test_get_all_nodes(self):
        ring = ConsistentHashRing(vnodes=10)
        ring.add_node("http://node1:8001")
        ring.add_node("http://node2:8002")
        all_nodes = ring.get_all_nodes()
        assert "http://node1:8001" in all_nodes
        assert "http://node2:8002" in all_nodes
        assert len(all_nodes) == 2

    def test_remove_nonexistent_node_safe(self):
        """Removing a non-existent node should not raise."""
        ring = ConsistentHashRing(vnodes=10)
        ring.add_node("http://node1:8001")
        ring.remove_node("http://nonexistent:9999")  # Should not raise
        assert ring.get_node("key") == "http://node1:8001"


class TestMessage:
    def test_message_creation(self):
        msg = Message(
            message_id="test-id",
            queue_name="orders",
            payload={"order": 123},
            producer_id="shop-service",
        )
        assert msg.message_id == "test-id"
        assert not msg.is_exhausted
        assert msg.attempts == 0

    def test_message_exhausted_after_max_attempts(self):
        msg = Message(
            message_id="test-id",
            queue_name="orders",
            payload={},
            producer_id="p1",
            attempts=3,
            max_attempts=3,
        )
        assert msg.is_exhausted

    def test_message_serialization(self):
        msg = Message(
            message_id="ser-id",
            queue_name="events",
            payload={"event": "click"},
            producer_id="web",
        )
        d = msg.to_dict()
        restored = Message.from_dict(d)
        assert restored.message_id == msg.message_id
        assert restored.payload == msg.payload
        assert restored.queue_name == msg.queue_name
        assert restored.producer_id == msg.producer_id

    def test_message_timestamp(self):
        before = time.time()
        msg = Message(
            message_id="ts-id",
            queue_name="q",
            payload={},
            producer_id="p",
        )
        after = time.time()
        assert before <= msg.created_at <= after
