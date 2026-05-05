"""Unit tests for Raft Consensus Algorithm."""
import asyncio
import pytest
from src.consensus.raft import RaftNode, RaftRole, LogEntry


@pytest.fixture
def node():
    return RaftNode("node1", ["http://node2:8002", "http://node3:8003"])


@pytest.mark.asyncio
async def test_initial_state(node):
    """Node should start as follower with term 0."""
    assert node.role == RaftRole.FOLLOWER
    assert node.current_term == 0
    assert node.voted_for is None
    assert node.leader_id is None
    assert len(node.log) == 0


@pytest.mark.asyncio
async def test_request_vote_grants_vote_when_log_up_to_date(node):
    """Should grant vote to candidate with up-to-date log."""
    req = {
        "term": 1,
        "candidate_id": "node2",
        "last_log_index": -1,
        "last_log_term": 0,
    }
    resp = await node.handle_request_vote(req)
    assert resp["vote_granted"] is True
    assert node.voted_for == "node2"
    assert node.current_term == 1


@pytest.mark.asyncio
async def test_request_vote_rejects_stale_term(node):
    """Should reject vote from candidate with lower term."""
    node.current_term = 5
    req = {
        "term": 3,
        "candidate_id": "node2",
        "last_log_index": -1,
        "last_log_term": 0,
    }
    resp = await node.handle_request_vote(req)
    assert resp["vote_granted"] is False


@pytest.mark.asyncio
async def test_request_vote_only_once_per_term(node):
    """Should only grant vote once per term."""
    req = {
        "term": 1,
        "candidate_id": "node2",
        "last_log_index": -1,
        "last_log_term": 0,
    }
    resp1 = await node.handle_request_vote(req)
    assert resp1["vote_granted"] is True

    # Different candidate, same term
    req2 = dict(req, candidate_id="node3")
    resp2 = await node.handle_request_vote(req2)
    assert resp2["vote_granted"] is False


@pytest.mark.asyncio
async def test_append_entries_valid_heartbeat(node):
    """Valid AppendEntries should reset election timeout and stay follower."""
    req = {
        "term": 1,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": [],
        "leader_commit": -1,
    }
    resp = await node.handle_append_entries(req)
    assert resp["success"] is True
    assert node.leader_id == "node2"
    assert node.current_term == 1


@pytest.mark.asyncio
async def test_append_entries_rejects_stale_leader(node):
    """Should reject AppendEntries from old term."""
    node.current_term = 5
    req = {
        "term": 3,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": [],
        "leader_commit": -1,
    }
    resp = await node.handle_append_entries(req)
    assert resp["success"] is False


@pytest.mark.asyncio
async def test_append_entries_adds_log_entries(node):
    """Should correctly append new log entries."""
    entry = LogEntry(term=1, index=0, command={"type": "TEST"})
    req = {
        "term": 1,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": [entry.to_dict()],
        "leader_commit": 0,
    }
    resp = await node.handle_append_entries(req)
    assert resp["success"] is True
    assert len(node.log) == 1
    assert node.log[0].command == {"type": "TEST"}


@pytest.mark.asyncio
async def test_step_down_on_higher_term(node):
    """Leader should step down when seeing higher term."""
    node.role = RaftRole.LEADER
    node.current_term = 3

    req = {
        "term": 5,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": [],
        "leader_commit": -1,
    }
    await node.handle_append_entries(req)
    assert node.role == RaftRole.FOLLOWER
    assert node.current_term == 5


def test_majority_calculation():
    """Majority should be ceil((n+1)/2) for n peers."""
    node1 = RaftNode("n1", ["n2", "n3"])           # 3 nodes → majority = 2
    node2 = RaftNode("n1", ["n2", "n3", "n4", "n5"])  # 5 nodes → majority = 3
    assert node1._majority() == 2
    assert node2._majority() == 3


def test_log_index_properties(node):
    """Test last_log_index and last_log_term on empty log."""
    assert node.last_log_index == -1
    assert node.last_log_term == 0

    node.log.append(LogEntry(term=2, index=0, command={}))
    assert node.last_log_index == 0
    assert node.last_log_term == 2
