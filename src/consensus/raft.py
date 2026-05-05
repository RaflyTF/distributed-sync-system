"""
Implementasi Raft Consensus Algorithm.
Paper: "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout, 2014)

Komponen yang diimplementasikan:
1. Leader Election (randomized election timeout)
2. Log Replication (AppendEntries RPC)
3. Safety (commit hanya jika majority nodes setuju)
4. Membership changes (simplified)
"""
import asyncio
import random
import time
import json
import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, Any
from src.communication.message_passing import MessagePassing
from src.utils.config import config
from src.utils.metrics import (
    raft_term, raft_role, raft_log_size, raft_elections_total, MetricsTracker
)

logger = logging.getLogger(__name__)


class RaftRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """Single entry in the Raft replicated log."""
    term: int
    index: int
    command: dict  # {"type": "LOCK", "resource": "...", ...}
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LogEntry":
        return cls(**d)


@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[dict]  # Serialized LogEntry
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int
    node_id: str


class RaftNode:
    """
    Raft Consensus Node.
    
    States persisted (would be on disk in production):
    - current_term: Latest term seen
    - voted_for: CandidateId voted for in current term
    - log: Log entries
    
    Volatile state:
    - commit_index: Highest log entry known to be committed
    - last_applied: Highest log entry applied to state machine
    
    Leader volatile state:
    - next_index[peer]: Next log index to send to each peer
    - match_index[peer]: Highest log index replicated on each peer
    """

    def __init__(self, node_id: str, peers: list[str]):
        self.node_id = node_id
        self.peers = peers
        self._messenger = MessagePassing(node_id)

        # Persistent state (on stable storage in production)
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: list[LogEntry] = []

        # Volatile state
        self.commit_index: int = -1
        self.last_applied: int = -1
        self.role: RaftRole = RaftRole.FOLLOWER
        self.leader_id: Optional[str] = None

        # Leader volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # Timing
        self._election_deadline: float = 0.0
        self._reset_election_timeout()
        self._running = False
        self._tasks: list[asyncio.Task] = []

        # State machine (committed commands are applied here)
        self._state_machine: dict[str, Any] = {}
        self._apply_callbacks: list = []

        # Metrics
        self._metrics = MetricsTracker(node_id)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def last_log_index(self) -> int:
        return len(self.log) - 1

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _majority(self) -> int:
        return (len(self.peers) + 2) // 2  # n+1 nodes total, need majority

    # ── Timeout Management ───────────────────────────────────────────────────

    def _reset_election_timeout(self):
        """Randomized election timeout: 150-300ms (converted to seconds)."""
        timeout_ms = random.randint(
            config.RAFT_ELECTION_TIMEOUT_MIN,
            config.RAFT_ELECTION_TIMEOUT_MAX,
        )
        self._election_deadline = time.time() + timeout_ms / 1000

    def _update_term(self, new_term: int):
        """Step down to follower if we see a higher term."""
        if new_term > self.current_term:
            self.current_term = new_term
            self.voted_for = None
            self._become_follower()

    # ── Role Transitions ─────────────────────────────────────────────────────

    def _become_follower(self):
        self.role = RaftRole.FOLLOWER
        self._update_metrics()
        logger.info(f"[Raft:{self.node_id}] → FOLLOWER (term={self.current_term})")

    def _become_candidate(self):
        self.current_term += 1
        self.voted_for = self.node_id
        self.role = RaftRole.CANDIDATE
        self.leader_id = None
        self._reset_election_timeout()
        raft_elections_total.labels(node_id=self.node_id).inc()
        self._update_metrics()
        logger.info(f"[Raft:{self.node_id}] → CANDIDATE (term={self.current_term})")

    def _become_leader(self):
        self.role = RaftRole.LEADER
        self.leader_id = self.node_id
        # Initialize leader state
        for peer in self.peers:
            self.next_index[peer] = self.last_log_index + 1
            self.match_index[peer] = -1
        self._update_metrics()
        logger.info(f"[Raft:{self.node_id}] → LEADER (term={self.current_term}) 👑")

    def _update_metrics(self):
        self._metrics.update_raft_state(
            self.current_term, self.role.value, len(self.log)
        )

    # ── RPC Handlers (called by HTTP endpoints) ──────────────────────────────

    async def handle_request_vote(self, req: dict) -> dict:
        """Handle RequestVote RPC."""
        term = req["term"]
        candidate_id = req["candidate_id"]
        last_log_index = req["last_log_index"]
        last_log_term = req["last_log_term"]

        self._update_term(term)

        vote_granted = False
        if term < self.current_term:
            pass  # Reject stale requests
        elif self.voted_for in (None, candidate_id):
            # Check if candidate's log is at least as up-to-date as ours
            log_ok = (
                last_log_term > self.last_log_term
                or (last_log_term == self.last_log_term and last_log_index >= self.last_log_index)
            )
            if log_ok:
                self.voted_for = candidate_id
                self._reset_election_timeout()
                vote_granted = True

        logger.debug(
            f"[Raft:{self.node_id}] VoteRequest from {candidate_id}: granted={vote_granted}"
        )
        return {"term": self.current_term, "vote_granted": vote_granted, "voter_id": self.node_id}

    async def handle_append_entries(self, req: dict) -> dict:
        """Handle AppendEntries RPC (also serves as heartbeat)."""
        term = req["term"]
        leader_id = req["leader_id"]
        prev_log_index = req["prev_log_index"]
        prev_log_term = req["prev_log_term"]
        entries = [LogEntry.from_dict(e) for e in req["entries"]]
        leader_commit = req["leader_commit"]

        self._update_term(term)
        success = False
        match_index = -1

        if term < self.current_term:
            return {"term": self.current_term, "success": False, "match_index": -1, "node_id": self.node_id}

        # Valid leader heartbeat
        self._reset_election_timeout()
        if self.role != RaftRole.FOLLOWER:
            self._become_follower()
        self.leader_id = leader_id

        # Check prev_log consistency
        if prev_log_index == -1:
            consistent = True
        elif prev_log_index < len(self.log):
            consistent = self.log[prev_log_index].term == prev_log_term
        else:
            consistent = False

        if consistent:
            # Append new entries (delete conflicts first)
            insert_idx = prev_log_index + 1
            for i, entry in enumerate(entries):
                log_pos = insert_idx + i
                if log_pos < len(self.log):
                    if self.log[log_pos].term != entry.term:
                        self.log = self.log[:log_pos]
                        self.log.append(entry)
                    # else: already have it
                else:
                    self.log.append(entry)

            # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.last_log_index)
                await self._apply_committed_entries()

            success = True
            match_index = self.last_log_index

        self._update_metrics()
        return {
            "term": self.current_term,
            "success": success,
            "match_index": match_index,
            "node_id": self.node_id,
        }

    # ── Leader Operations ─────────────────────────────────────────────────────

    async def propose(self, command: dict) -> Optional[int]:
        """
        Propose a command to the replicated log.
        Returns log index if leader, None if not leader.
        """
        if self.role != RaftRole.LEADER:
            return None

        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command,
        )
        self.log.append(entry)
        self._update_metrics()
        logger.info(f"[Raft:{self.node_id}] Proposed command at index {entry.index}: {command}")

        # Replicate to peers
        success_count = 1  # Count self
        replication_tasks = [
            self._replicate_to_peer(peer) for peer in self.peers
        ]
        results = await asyncio.gather(*replication_tasks, return_exceptions=True)
        success_count += sum(1 for r in results if r is True)

        # Commit if majority
        if success_count >= self._majority():
            self.commit_index = entry.index
            await self._apply_committed_entries()
            logger.info(f"[Raft:{self.node_id}] Committed entry {entry.index} ({success_count}/{len(self.peers)+1} nodes)")
            return entry.index

        logger.warning(f"[Raft:{self.node_id}] Failed to reach majority for entry {entry.index}")
        return None

    async def _replicate_to_peer(self, peer: str) -> bool:
        """Replicate log entries to a single peer."""
        next_idx = self.next_index.get(peer, 0)
        prev_idx = next_idx - 1
        prev_term = self.log[prev_idx].term if prev_idx >= 0 and prev_idx < len(self.log) else 0
        entries_to_send = [e.to_dict() for e in self.log[next_idx:]]

        req = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term": prev_term,
            "entries": entries_to_send,
            "leader_commit": self.commit_index,
        }
        resp = await self._messenger.send(peer, "/raft/append-entries", req)
        if resp is None:
            return False

        if resp.get("term", 0) > self.current_term:
            self._update_term(resp["term"])
            return False

        if resp.get("success"):
            self.match_index[peer] = resp.get("match_index", -1)
            self.next_index[peer] = self.match_index[peer] + 1
            return True
        else:
            # Back off next_index and retry
            self.next_index[peer] = max(0, next_idx - 1)
            return False

    async def _send_heartbeats(self):
        """Send empty AppendEntries to all peers (heartbeat)."""
        if self.role != RaftRole.LEADER:
            return
        tasks = [
            self._messenger.send(
                peer,
                "/raft/append-entries",
                {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": self.last_log_index,
                    "prev_log_term": self.last_log_term,
                    "entries": [],
                    "leader_commit": self.commit_index,
                },
            )
            for peer in self.peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for peer, resp in zip(self.peers, results):
            if isinstance(resp, dict) and resp.get("term", 0) > self.current_term:
                self._update_term(resp["term"])
                break
            if isinstance(resp, dict) and resp.get("success"):
                self._messenger._get_circuit_breaker(peer).record_success()

    async def _start_election(self):
        """Start a new leader election."""
        self._become_candidate()
        votes = 1  # Vote for self

        req = {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
        }
        results = await self._messenger.broadcast(self.peers, "/raft/request-vote", req)

        for peer, resp in results.items():
            if resp and resp.get("term", 0) > self.current_term:
                self._update_term(resp["term"])
                return
            if resp and resp.get("vote_granted"):
                votes += 1

        logger.info(f"[Raft:{self.node_id}] Election result: {votes}/{len(self.peers)+1} votes")
        if votes >= self._majority() and self.role == RaftRole.CANDIDATE:
            self._become_leader()
            await self._send_heartbeats()

    async def _apply_committed_entries(self):
        """Apply committed log entries to the state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            await self._apply_to_state_machine(entry)

    async def _apply_to_state_machine(self, entry: LogEntry):
        """Apply a single log entry to the state machine."""
        for callback in self._apply_callbacks:
            try:
                await callback(entry.command)
            except Exception as e:
                logger.error(f"[Raft:{self.node_id}] State machine error: {e}")

    def on_commit(self, callback):
        """Register callback for when entries are committed."""
        self._apply_callbacks.append(callback)

    # ── Main Loop ─────────────────────────────────────────────────────────────

    async def _election_loop(self):
        """Check election timeout and trigger elections."""
        while self._running:
            await asyncio.sleep(0.01)  # Check every 10ms
            if self.role != RaftRole.LEADER:
                if time.time() > self._election_deadline:
                    await self._start_election()

    async def _heartbeat_loop(self):
        """Leader sends heartbeats at regular intervals."""
        interval = config.RAFT_HEARTBEAT_INTERVAL / 1000
        while self._running:
            await asyncio.sleep(interval)
            if self.role == RaftRole.LEADER:
                await self._send_heartbeats()

    async def start(self):
        """Start the Raft node."""
        self._running = True
        self._tasks = [
            asyncio.create_task(self._election_loop()),
            asyncio.create_task(self._heartbeat_loop()),
        ]
        logger.info(f"[Raft:{self.node_id}] Started with {len(self.peers)} peers")

    async def stop(self):
        """Stop the Raft node."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await self._messenger.close()

    def get_status(self) -> dict:
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "log_size": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "peers": self.peers,
        }
