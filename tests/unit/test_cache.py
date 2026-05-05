"""Unit tests for MESI Cache Coherence Protocol and LRU replacement policy."""
import asyncio
try:
    import pytest  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover - dependency guard for test discovery
    raise RuntimeError("pytest is required to run these tests") from exc
from unittest.mock import AsyncMock, patch
from src.nodes.cache_node import MESICacheNode, LRUCache, CacheLine, MESIState


# ── LRU Cache Tests ──────────────────────────────────────────────────────────

class TestLRUCache:
    def test_empty_cache_returns_none(self):
        lru = LRUCache(capacity=3)
        assert lru.get("missing") is None

    def test_put_and_get(self):
        lru = LRUCache(capacity=3)
        line = CacheLine(key="k1", value="v1", state=MESIState.EXCLUSIVE)
        lru.put("k1", line)
        result = lru.get("k1")
        assert result is not None
        assert result.value == "v1"

    def test_lru_eviction_order(self):
        """Oldest item should be evicted when capacity exceeded."""
        lru = LRUCache(capacity=3)
        for i in range(3):
            lru.put(f"k{i}", CacheLine(key=f"k{i}", value=i, state=MESIState.SHARED))

        # Access k0 to make it recently used
        lru.get("k0")

        # Add k3 → k1 should be evicted (LRU)
        evicted = lru.put("k3", CacheLine(key="k3", value=3, state=MESIState.SHARED))
        assert evicted == "k1"
        assert lru.get("k1") is None
        assert lru.get("k0") is not None  # Was accessed, so not evicted

    def test_update_existing_key(self):
        lru = LRUCache(capacity=3)
        lru.put("k1", CacheLine(key="k1", value="old", state=MESIState.SHARED))
        lru.put("k1", CacheLine(key="k1", value="new", state=MESIState.MODIFIED))
        line = lru.get("k1")
        assert line is not None
        assert line.value == "new"
        assert len(lru) == 1

    def test_invalidate_removes_key(self):
        lru = LRUCache(capacity=5)
        lru.put("k1", CacheLine(key="k1", value="v1", state=MESIState.SHARED))
        assert lru.invalidate("k1") is True
        assert lru.get("k1") is None
        assert "k1" not in lru

    def test_invalidate_missing_key_returns_false(self):
        lru = LRUCache(capacity=5)
        assert lru.invalidate("nonexistent") is False

    def test_capacity_is_respected(self):
        lru = LRUCache(capacity=2)
        lru.put("k1", CacheLine(key="k1", value=1, state=MESIState.SHARED))
        lru.put("k2", CacheLine(key="k2", value=2, state=MESIState.SHARED))
        lru.put("k3", CacheLine(key="k3", value=3, state=MESIState.SHARED))
        assert len(lru) == 2

    def test_update_state(self):
        lru = LRUCache(capacity=5)
        lru.put("k1", CacheLine(key="k1", value="v", state=MESIState.SHARED))
        lru.update_state("k1", MESIState.MODIFIED)
        line = lru.get("k1")
        assert line is not None
        assert line.state == MESIState.MODIFIED


# ── MESI Cache Node Tests ────────────────────────────────────────────────────

class TestMESICacheNode:
    @pytest.fixture
    def cache_node(self):
        # Clear shared memory between tests
        MESICacheNode._MEMORY.clear()
        node = MESICacheNode("node1", [], capacity=10)
        return node

    @pytest.mark.asyncio
    async def test_write_creates_modified_state(self, cache_node):
        await cache_node.write("key1", "value1")
        lines = cache_node.get_cache_lines()
        assert len(lines) == 1
        assert lines[0]["state"] == MESIState.MODIFIED
        assert lines[0]["key"] == "key1"

    @pytest.mark.asyncio
    async def test_read_after_write_hits_cache(self, cache_node):
        await cache_node.write("key2", {"data": 42})
        result = await cache_node.read("key2")
        assert result == {"data": 42}

    @pytest.mark.asyncio
    async def test_read_miss_returns_none_when_not_in_memory(self, cache_node):
        result = await cache_node.read("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_read_from_memory_on_cache_miss(self, cache_node):
        """If not in cache but in memory, should fetch from memory."""
        MESICacheNode._MEMORY["mem-key"] = {"value": "from-memory", "version": 1}
        result = await cache_node.read("mem-key")
        assert result == "from-memory"

    @pytest.mark.asyncio
    async def test_write_updates_memory(self, cache_node):
        await cache_node.write("key3", "hello")
        assert "key3" in MESICacheNode._MEMORY
        assert MESICacheNode._MEMORY["key3"]["value"] == "hello"

    @pytest.mark.asyncio
    async def test_write_increments_version(self, cache_node):
        await cache_node.write("key4", "v1")
        v1 = MESICacheNode._MEMORY["key4"]["version"]
        await cache_node.write("key4", "v2")
        v2 = MESICacheNode._MEMORY["key4"]["version"]
        assert v2 == v1 + 1

    @pytest.mark.asyncio
    async def test_delete_removes_from_cache_and_memory(self, cache_node):
        await cache_node.write("del-key", "temp")
        await cache_node.delete("del-key")
        result = await cache_node.read("del-key")
        assert result is None
        assert "del-key" not in MESICacheNode._MEMORY

    @pytest.mark.asyncio
    async def test_handle_invalidate_removes_line(self, cache_node):
        await cache_node.write("inv-key", "data")
        resp = await cache_node.handle_invalidate("inv-key", "node2")
        assert resp["invalidated"] is True
        lines = cache_node.get_cache_lines()
        assert not any(l["key"] == "inv-key" for l in lines)

    @pytest.mark.asyncio
    async def test_handle_invalidate_missing_key(self, cache_node):
        resp = await cache_node.handle_invalidate("missing-key", "node2")
        assert resp["invalidated"] is False

    @pytest.mark.asyncio
    async def test_handle_fetch_returns_value(self, cache_node):
        await cache_node.write("fetch-key", "fetch-value")
        resp = await cache_node.handle_fetch("fetch-key", "node2")
        assert resp["found"] is True
        assert resp["value"] == "fetch-value"

    @pytest.mark.asyncio
    async def test_handle_fetch_transitions_modified_to_shared(self, cache_node):
        """M state should transition to S when another node fetches."""
        await cache_node.write("m-key", "dirty-data")
        lines_before = {l["key"]: l for l in cache_node.get_cache_lines()}
        assert lines_before["m-key"]["state"] == MESIState.MODIFIED

        resp = await cache_node.handle_fetch("m-key", "node2")
        assert resp["found"] is True

        lines_after = {l["key"]: l for l in cache_node.get_cache_lines()}
        assert lines_after["m-key"]["state"] == MESIState.SHARED

    @pytest.mark.asyncio
    async def test_handle_fetch_missing_key(self, cache_node):
        resp = await cache_node.handle_fetch("no-key", "node2")
        assert resp["found"] is False

    @pytest.mark.asyncio
    async def test_cache_status_structure(self, cache_node):
        await cache_node.write("status-key", "val")
        status = cache_node.get_status()
        assert status["node_id"] == "node1"
        assert status["protocol"] == "MESI"
        assert status["policy"] == "LRU"
        assert "state_distribution" in status
        assert status["size"] >= 1

    @pytest.mark.asyncio
    async def test_multiple_writes_overwrite(self, cache_node):
        await cache_node.write("ow-key", "first")
        await cache_node.write("ow-key", "second")
        result = await cache_node.read("ow-key")
        assert result == "second"
