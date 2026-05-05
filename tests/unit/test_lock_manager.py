"""Unit tests for Distributed Lock Manager and Resource Allocation Graph."""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.nodes.lock_manager import (
    DistributedLockManager, LockType, LockStatus, ResourceAllocationGraph, LockEntry
)
from src.consensus.raft import RaftNode, RaftRole


@pytest.fixture
def mock_raft():
    raft = MagicMock(spec=RaftNode)
    raft.node_id = "node1"
    raft.role = MagicMock()
    raft.role.value = "leader"
    raft.peers = []
    raft.propose = AsyncMock(return_value=0)
    raft._apply_callbacks = []

    def on_commit(cb):
        raft._apply_callbacks.append(cb)
    raft.on_commit = on_commit
    return raft


@pytest.fixture
def lock_mgr(mock_raft):
    return DistributedLockManager(mock_raft)


# ── Resource Allocation Graph Tests ──────────────────────────────────────────

class TestResourceAllocationGraph:
    def test_no_deadlock_initially(self):
        rag = ResourceAllocationGraph()
        assert rag.detect_deadlock() is None

    def test_no_deadlock_simple_hold(self):
        rag = ResourceAllocationGraph()
        rag.add_hold("P1", "R1")
        assert rag.detect_deadlock() is None

    def test_detect_simple_deadlock(self):
        """P1 holds R1, waits for R2; P2 holds R2, waits for R1 → deadlock."""
        rag = ResourceAllocationGraph()
        rag.add_hold("P1", "R1")
        rag.add_hold("P2", "R2")
        rag.add_wait("P1", "R2")  # P1 → R2 → P2
        rag.add_wait("P2", "R1")  # P2 → R1 → P1 (cycle!)
        cycle = rag.detect_deadlock()
        assert cycle is not None
        assert len(cycle) >= 2

    def test_no_deadlock_chain_without_cycle(self):
        """P1 waits for R1 (held by P2), P2 not waiting → no deadlock."""
        rag = ResourceAllocationGraph()
        rag.add_hold("P2", "R1")
        rag.add_wait("P1", "R1")
        assert rag.detect_deadlock() is None

    def test_deadlock_resolved_after_release(self):
        rag = ResourceAllocationGraph()
        rag.add_hold("P1", "R1")
        rag.add_hold("P2", "R2")
        rag.add_wait("P1", "R2")
        rag.add_wait("P2", "R1")
        assert rag.detect_deadlock() is not None

        # P2 releases R1
        rag.remove_hold("P2", "R1")
        rag.remove_wait("P2", "R1")
        assert rag.detect_deadlock() is None

    def test_three_way_deadlock(self):
        """P1→R2→P2→R3→P3→R1→P1: three-way deadlock."""
        rag = ResourceAllocationGraph()
        rag.add_hold("P1", "R1")
        rag.add_hold("P2", "R2")
        rag.add_hold("P3", "R3")
        rag.add_wait("P1", "R2")
        rag.add_wait("P2", "R3")
        rag.add_wait("P3", "R1")
        cycle = rag.detect_deadlock()
        assert cycle is not None


# ── Lock Manager Tests ───────────────────────────────────────────────────────

class TestLockManager:
    @pytest.mark.asyncio
    async def test_acquire_exclusive_lock(self, lock_mgr):
        result = await lock_mgr.acquire("resource-A", LockType.EXCLUSIVE, "owner-1")
        assert result["status"] == LockStatus.GRANTED
        assert "lock_id" in result
        assert result["resource"] == "resource-A"

    @pytest.mark.asyncio
    async def test_exclusive_lock_blocks_second_exclusive(self, lock_mgr):
        """Second exclusive lock should be denied (no-wait mode)."""
        r1 = await lock_mgr.acquire("resource-B", LockType.EXCLUSIVE, "owner-1")
        assert r1["status"] == LockStatus.GRANTED

        r2 = await lock_mgr.acquire("resource-B", LockType.EXCLUSIVE, "owner-2", wait=False)
        assert r2["status"] == LockStatus.DENIED

    @pytest.mark.asyncio
    async def test_multiple_shared_locks_granted(self, lock_mgr):
        """Multiple shared locks on same resource should all be granted."""
        r1 = await lock_mgr.acquire("resource-C", LockType.SHARED, "reader-1")
        r2 = await lock_mgr.acquire("resource-C", LockType.SHARED, "reader-2")
        r3 = await lock_mgr.acquire("resource-C", LockType.SHARED, "reader-3")
        assert r1["status"] == LockStatus.GRANTED
        assert r2["status"] == LockStatus.GRANTED
        assert r3["status"] == LockStatus.GRANTED

    @pytest.mark.asyncio
    async def test_exclusive_blocks_shared(self, lock_mgr):
        """Exclusive lock should block new shared locks."""
        r1 = await lock_mgr.acquire("resource-D", LockType.EXCLUSIVE, "writer-1")
        assert r1["status"] == LockStatus.GRANTED

        r2 = await lock_mgr.acquire("resource-D", LockType.SHARED, "reader-1", wait=False)
        assert r2["status"] == LockStatus.DENIED

    @pytest.mark.asyncio
    async def test_shared_blocks_exclusive(self, lock_mgr):
        """Shared locks should block exclusive lock."""
        r1 = await lock_mgr.acquire("resource-E", LockType.SHARED, "reader-1")
        assert r1["status"] == LockStatus.GRANTED

        r2 = await lock_mgr.acquire("resource-E", LockType.EXCLUSIVE, "writer-1", wait=False)
        assert r2["status"] == LockStatus.DENIED

    @pytest.mark.asyncio
    async def test_release_lock(self, lock_mgr):
        r1 = await lock_mgr.acquire("resource-F", LockType.EXCLUSIVE, "owner-1")
        assert r1["status"] == LockStatus.GRANTED

        release = await lock_mgr.release(r1["lock_id"], "owner-1")
        assert release["status"] == LockStatus.RELEASED

        # Now another owner can acquire
        r2 = await lock_mgr.acquire("resource-F", LockType.EXCLUSIVE, "owner-2")
        assert r2["status"] == LockStatus.GRANTED

    @pytest.mark.asyncio
    async def test_release_wrong_owner_fails(self, lock_mgr):
        r1 = await lock_mgr.acquire("resource-G", LockType.EXCLUSIVE, "owner-1")
        release = await lock_mgr.release(r1["lock_id"], "wrong-owner")
        assert release["status"] == "not_found"

    @pytest.mark.asyncio
    async def test_lock_expiry(self, lock_mgr):
        """Expired lock should be removed on next access."""
        import time
        r1 = await lock_mgr.acquire("resource-H", LockType.EXCLUSIVE, "owner-1", timeout=0.001)
        assert r1["status"] == LockStatus.GRANTED

        await asyncio.sleep(0.01)  # Let it expire

        # Next acquire should succeed (expired lock auto-cleaned)
        r2 = await lock_mgr.acquire("resource-H", LockType.EXCLUSIVE, "owner-2", wait=False)
        assert r2["status"] == LockStatus.GRANTED

    @pytest.mark.asyncio
    async def test_lock_status_shows_active_locks(self, lock_mgr):
        await lock_mgr.acquire("res-1", LockType.EXCLUSIVE, "owner-1")
        await lock_mgr.acquire("res-2", LockType.SHARED, "owner-2")
        status = lock_mgr.get_status()
        assert status["active_locks"] >= 2
        assert "rag" in status


# ── LockEntry Tests ──────────────────────────────────────────────────────────

def test_lock_entry_expiry():
    import time
    entry = LockEntry("id1", "res", LockType.EXCLUSIVE, "owner", timeout=0.001)
    assert not entry.is_expired
    import time; time.sleep(0.01)
    assert entry.is_expired


def test_lock_entry_not_expired():
    entry = LockEntry("id1", "res", LockType.SHARED, "owner", timeout=30.0)
    assert not entry.is_expired
