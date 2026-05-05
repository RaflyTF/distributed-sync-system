"""
Distributed Lock Manager berbasis Raft Consensus.
Mendukung:
- Shared Locks (multiple readers)
- Exclusive Locks (single writer)
- Deadlock Detection menggunakan Resource Allocation Graph (RAG)
- Lock Timeout & Auto-release
"""
import asyncio
import time
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from src.consensus.raft import RaftNode
from src.utils.metrics import (
    lock_requests_total, lock_duration_seconds, active_locks, deadlock_detected_total
)
from src.utils.config import config

logger = logging.getLogger(__name__)


class LockType(str, Enum):
    SHARED = "shared"        # Multiple readers allowed
    EXCLUSIVE = "exclusive"  # Only one writer allowed


class LockStatus(str, Enum):
    GRANTED = "granted"
    WAITING = "waiting"
    DENIED = "denied"
    RELEASED = "released"
    DEADLOCK = "deadlock"


@dataclass
class LockEntry:
    """Represents an acquired lock."""
    lock_id: str
    resource: str
    lock_type: LockType
    owner: str           # Client/process ID
    acquired_at: float = field(default_factory=time.time)
    timeout: float = 30.0  # Auto-release after N seconds

    @property
    def is_expired(self) -> bool:
        return time.time() - self.acquired_at > self.timeout

    def to_dict(self) -> dict:
        return {
            "lock_id": self.lock_id,
            "resource": self.resource,
            "lock_type": self.lock_type,
            "owner": self.owner,
            "acquired_at": self.acquired_at,
            "timeout": self.timeout,
            "is_expired": self.is_expired,
        }


class ResourceAllocationGraph:
    """
    Resource Allocation Graph untuk deadlock detection.
    
    Nodes: processes (owners) + resources
    Edges:
    - Assignment edge: resource → process (resource assigned to process)
    - Request edge: process → resource (process waiting for resource)
    
    Deadlock = cycle in the graph
    """

    def __init__(self):
        # process → list of resources it's waiting for
        self._waiting: dict[str, set[str]] = defaultdict(set)
        # resource → list of processes holding it
        self._holding: dict[str, set[str]] = defaultdict(set)

    def add_hold(self, owner: str, resource: str):
        self._holding[resource].add(owner)

    def remove_hold(self, owner: str, resource: str):
        self._holding[resource].discard(owner)

    def add_wait(self, owner: str, resource: str):
        self._waiting[owner].add(resource)

    def remove_wait(self, owner: str, resource: str):
        self._waiting[owner].discard(resource)

    def detect_deadlock(self) -> Optional[list[str]]:
        """
        DFS-based cycle detection in the Resource Allocation Graph.
        Returns the cycle (list of nodes) if found, None otherwise.
        """
        visited: set[str] = set()
        path: list[str] = []
        path_set: set[str] = set()

        def dfs(node: str) -> Optional[list[str]]:
            if node in path_set:
                # Found cycle — extract it
                cycle_start = path.index(node)
                return path[cycle_start:]
            if node in visited:
                return None
            visited.add(node)
            path.append(node)
            path_set.add(node)

            # If node is a process: follow resources it's waiting for
            for resource in self._waiting.get(node, set()):
                # Follow resource to processes holding it
                for holder in self._holding.get(resource, set()):
                    result = dfs(holder)
                    if result:
                        return result

            path.pop()
            path_set.discard(node)
            return None

        all_owners = set(self._waiting.keys())
        for owner in all_owners:
            if owner not in visited:
                cycle = dfs(owner)
                if cycle:
                    return cycle

        return None

    def get_graph_state(self) -> dict:
        return {
            "holding": {r: list(owners) for r, owners in self._holding.items()},
            "waiting": {p: list(resources) for p, resources in self._waiting.items()},
        }


class DistributedLockManager:
    """
    Distributed Lock Manager.
    
    Semua lock decisions direplikasi melalui Raft untuk konsistensi.
    Deadlock detection berjalan secara periodik di background.
    """

    def __init__(self, raft: RaftNode):
        self.raft = raft
        self.node_id = raft.node_id
        self._locks: dict[str, list[LockEntry]] = defaultdict(list)  # resource → locks
        self._waiting_queue: dict[str, asyncio.Queue] = {}
        self._rag = ResourceAllocationGraph()
        self._running = False
        self._cleanup_task: Optional[asyncio.Task] = None
        self._deadlock_task: Optional[asyncio.Task] = None
        self._lock_counter = 0

        # Register Raft commit callback
        self.raft.on_commit(self._apply_lock_command)

    def _generate_lock_id(self) -> str:
        self._lock_counter += 1
        return f"{self.node_id}:lock:{self._lock_counter}:{int(time.time())}"

    # ── Lock Acquisition ──────────────────────────────────────────────────────

    async def acquire(
        self,
        resource: str,
        lock_type: LockType,
        owner: str,
        timeout: float = 30.0,
        wait: bool = True,
    ) -> dict:
        """
        Acquire a lock on a resource.
        
        Args:
            resource: Resource identifier
            lock_type: SHARED or EXCLUSIVE
            owner: Process/client requesting the lock
            timeout: Auto-release timeout in seconds
            wait: Whether to wait if lock is not immediately available
        """
        lock_requests_total.labels(
            node_id=self.node_id, lock_type=lock_type, status="requested"
        ).inc()

        # Check if we can grant immediately
        can_grant, reason = self._can_grant(resource, lock_type, owner)

        if can_grant:
            return await self._grant_lock(resource, lock_type, owner, timeout)

        if not wait:
            lock_requests_total.labels(
                node_id=self.node_id, lock_type=lock_type, status="denied"
            ).inc()
            return {"status": LockStatus.DENIED, "reason": reason}

        # Wait for lock
        logger.info(f"[LockMgr:{self.node_id}] {owner} waiting for {lock_type} lock on {resource}")
        self._rag.add_wait(owner, resource)

        # Check for deadlock before waiting
        cycle = self._rag.detect_deadlock()
        if cycle:
            self._rag.remove_wait(owner, resource)
            deadlock_detected_total.labels(node_id=self.node_id).inc()
            logger.warning(f"[LockMgr:{self.node_id}] DEADLOCK detected: {cycle}")
            lock_requests_total.labels(
                node_id=self.node_id, lock_type=lock_type, status="deadlock"
            ).inc()
            return {"status": LockStatus.DEADLOCK, "cycle": cycle}

        # Poll for lock availability
        deadline = time.time() + timeout
        while time.time() < deadline:
            await asyncio.sleep(0.05)
            can_grant, _ = self._can_grant(resource, lock_type, owner)
            if can_grant:
                self._rag.remove_wait(owner, resource)
                return await self._grant_lock(resource, lock_type, owner, timeout)

            # Re-check deadlock periodically
            cycle = self._rag.detect_deadlock()
            if cycle:
                self._rag.remove_wait(owner, resource)
                deadlock_detected_total.labels(node_id=self.node_id).inc()
                return {"status": LockStatus.DEADLOCK, "cycle": cycle}

        self._rag.remove_wait(owner, resource)
        lock_requests_total.labels(
            node_id=self.node_id, lock_type=lock_type, status="timeout"
        ).inc()
        return {"status": LockStatus.DENIED, "reason": "timeout"}

    def _can_grant(self, resource: str, lock_type: LockType, owner: str) -> tuple[bool, str]:
        """Check if a lock can be granted given current state."""
        existing = self._locks.get(resource, [])
        # Filter out expired locks
        existing = [l for l in existing if not l.is_expired]
        self._locks[resource] = existing

        if not existing:
            return True, "no existing locks"

        if lock_type == LockType.SHARED:
            # Shared lock can be granted if only shared locks exist
            exclusive = [l for l in existing if l.lock_type == LockType.EXCLUSIVE]
            if exclusive and exclusive[0].owner != owner:
                return False, f"exclusive lock held by {exclusive[0].owner}"
            return True, "shared locks compatible"

        else:  # EXCLUSIVE
            holders = [l for l in existing if l.owner != owner]
            if holders:
                return False, f"lock held by {[l.owner for l in holders]}"
            return True, "no conflicting locks"

    async def _grant_lock(
        self, resource: str, lock_type: LockType, owner: str, timeout: float
    ) -> dict:
        """Grant a lock and replicate via Raft."""
        lock_id = self._generate_lock_id()

        # Replicate through Raft if we're leader
        if self.raft.role.value == "leader":
            command = {
                "type": "ACQUIRE_LOCK",
                "lock_id": lock_id,
                "resource": resource,
                "lock_type": lock_type,
                "owner": owner,
                "timeout": timeout,
                "timestamp": time.time(),
            }
            index = await self.raft.propose(command)
            if index is None and len(self.raft.peers) > 0:
                logger.warning("[LockMgr] Raft propose failed, applying locally")

        # Apply locally
        entry = LockEntry(
            lock_id=lock_id,
            resource=resource,
            lock_type=lock_type,
            owner=owner,
            timeout=timeout,
        )
        self._locks[resource].append(entry)
        self._rag.add_hold(owner, resource)

        active_locks.labels(node_id=self.node_id, lock_type=lock_type).inc()
        lock_requests_total.labels(
            node_id=self.node_id, lock_type=lock_type, status="granted"
        ).inc()

        logger.info(f"[LockMgr:{self.node_id}] GRANTED {lock_type} lock on '{resource}' to {owner} (id={lock_id})")
        return {"status": LockStatus.GRANTED, "lock_id": lock_id, "resource": resource, "lock_type": lock_type}

    # ── Lock Release ──────────────────────────────────────────────────────────

    async def release(self, lock_id: str, owner: str) -> dict:
        """Release a lock by ID."""
        for resource, entries in self._locks.items():
            for entry in entries:
                if entry.lock_id == lock_id and entry.owner == owner:
                    entries.remove(entry)
                    self._rag.remove_hold(owner, resource)

                    # Replicate release via Raft
                    if self.raft.role.value == "leader":
                        await self.raft.propose({
                            "type": "RELEASE_LOCK",
                            "lock_id": lock_id,
                            "resource": resource,
                            "owner": owner,
                        })

                    active_locks.labels(
                        node_id=self.node_id, lock_type=entry.lock_type
                    ).dec()
                    logger.info(f"[LockMgr:{self.node_id}] RELEASED lock {lock_id} on '{resource}' by {owner}")
                    return {"status": LockStatus.RELEASED, "lock_id": lock_id}

        return {"status": "not_found", "lock_id": lock_id}

    async def _apply_lock_command(self, command: dict):
        """Apply committed Raft commands to local lock state."""
        cmd_type = command.get("type")
        if cmd_type == "ACQUIRE_LOCK":
            resource = command["resource"]
            # Only apply if not already present (idempotent)
            existing_ids = {l.lock_id for l in self._locks.get(resource, [])}
            if command["lock_id"] not in existing_ids:
                entry = LockEntry(
                    lock_id=command["lock_id"],
                    resource=resource,
                    lock_type=command["lock_type"],
                    owner=command["owner"],
                    timeout=command["timeout"],
                    acquired_at=command["timestamp"],
                )
                self._locks[resource].append(entry)
        elif cmd_type == "RELEASE_LOCK":
            resource = command.get("resource")
            lock_id = command.get("lock_id")
            if resource and lock_id:
                self._locks[resource] = [
                    l for l in self._locks.get(resource, []) if l.lock_id != lock_id
                ]

    # ── Background Tasks ──────────────────────────────────────────────────────

    async def _cleanup_expired_locks(self):
        """Periodically clean up expired locks."""
        while self._running:
            await asyncio.sleep(5)
            for resource in list(self._locks.keys()):
                expired = [l for l in self._locks[resource] if l.is_expired]
                for lock in expired:
                    logger.info(f"[LockMgr:{self.node_id}] Auto-releasing expired lock {lock.lock_id}")
                    await self.release(lock.lock_id, lock.owner)

    async def _periodic_deadlock_detection(self):
        """Run deadlock detection every second."""
        while self._running:
            await asyncio.sleep(1)
            cycle = self._rag.detect_deadlock()
            if cycle:
                deadlock_detected_total.labels(node_id=self.node_id).inc()
                logger.warning(f"[LockMgr:{self.node_id}] Periodic deadlock detection found cycle: {cycle}")
                # Victim selection: break cycle by releasing the newest lock
                # (simplified: just log it; in production, would abort a transaction)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_locks())
        self._deadlock_task = asyncio.create_task(self._periodic_deadlock_detection())
        logger.info(f"[LockMgr:{self.node_id}] Started")

    async def stop(self):
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._deadlock_task:
            self._deadlock_task.cancel()

    def get_status(self) -> dict:
        all_locks = []
        for resource, entries in self._locks.items():
            for entry in entries:
                if not entry.is_expired:
                    all_locks.append(entry.to_dict())
        return {
            "node_id": self.node_id,
            "active_locks": len(all_locks),
            "locks": all_locks,
            "rag": self._rag.get_graph_state(),
        }
