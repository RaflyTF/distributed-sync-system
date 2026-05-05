"""
Distributed Cache dengan MESI Cache Coherence Protocol.

MESI States:
- M (Modified): Data dirty, hanya ada di cache ini, belum ditulis ke memory
- E (Exclusive): Data clean, hanya ada di cache ini (dari memory)
- S (Shared): Data clean, mungkin ada di banyak caches
- I (Invalid): Data tidak valid, harus fetch ulang

Transitions MESI:
  I → E: Read miss (no other caches have it)
  I → S: Read miss (other caches have it in S)
  I/S/E → M: Write (must invalidate all others)
  M → I: Another node reads (flush to memory, transition to S or E)
  E → S: Another node reads
  S → I: Another node writes
  M → S: Another node reads (writeback + share)
"""
import asyncio
import time
import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any
from src.communication.message_passing import MessagePassing
from src.utils.config import config
from src.utils.metrics import (
    cache_hits, cache_misses, cache_invalidations, cache_size, MetricsTracker
)

logger = logging.getLogger(__name__)


class MESIState(str, Enum):
    MODIFIED = "M"   # Dirty, exclusive
    EXCLUSIVE = "E"  # Clean, exclusive
    SHARED = "S"     # Clean, shared
    INVALID = "I"    # Invalid


@dataclass
class CacheLine:
    """A single cache line with MESI state."""
    key: str
    value: Any
    state: MESIState
    version: int = 0
    last_accessed: float = field(default_factory=time.time)
    last_modified: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value,
            "state": self.state.value,
            "version": self.version,
            "last_accessed": self.last_accessed,
            "last_modified": self.last_modified,
        }


class LRUCache:
    """
    LRU (Least Recently Used) Cache implementation menggunakan OrderedDict.
    O(1) untuk get dan put operations.
    """

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: OrderedDict[str, CacheLine] = OrderedDict()

    def get(self, key: str) -> Optional[CacheLine]:
        if key not in self._cache:
            return None
        # Move to end (most recently used)
        self._cache.move_to_end(key)
        line = self._cache[key]
        line.last_accessed = time.time()
        return line

    def put(self, key: str, line: CacheLine) -> Optional[str]:
        """Put a cache line. Returns evicted key if capacity exceeded."""
        evicted_key = None
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self.capacity:
                # Evict LRU (first item)
                evicted_key, _ = self._cache.popitem(last=False)
        self._cache[key] = line
        return evicted_key

    def invalidate(self, key: str) -> bool:
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def update_state(self, key: str, state: MESIState):
        if key in self._cache:
            self._cache[key].state = state

    def get_all_keys(self) -> list[str]:
        return list(self._cache.keys())

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: str) -> bool:
        return key in self._cache


class MESICacheNode:
    """
    Distributed Cache Node dengan MESI Cache Coherence Protocol.
    
    Komunikasi antar nodes:
    - READ_SHARE: Node lain ingin membaca (kita berikan copy, transisi ke S)
    - INVALIDATE: Node lain ingin menulis (kita invalidate copy kita)
    - WRITEBACK: Node mengirim dirty data ke memory sebelum invalidate
    - FLUSH: Node Modified mengirim data ke requester
    """

    # Simulated "main memory" - shared state across all nodes
    # In production, ini bisa Redis atau distributed KV store
    _MEMORY: dict[str, dict] = {}  # key → {value, version}

    def __init__(self, node_id: str, peers: list[str], capacity: int = None):
        self.node_id = node_id
        self.peers = peers
        self._lru = LRUCache(capacity or config.CACHE_MAX_SIZE)
        self._messenger = MessagePassing(node_id)
        self._metrics = MetricsTracker(node_id)
        self._running = False
        self._write_through = False  # Write-back policy (default)

    # ── Read Operation ────────────────────────────────────────────────────────

    async def read(self, key: str) -> Optional[Any]:
        """
        Read a value from the distributed cache.
        Implements MESI read protocol.
        """
        line = self._lru.get(key)

        if line and line.state != MESIState.INVALID:
            # Cache HIT
            self._metrics.record_cache_access(hit=True)
            cache_hits.labels(node_id=self.node_id).inc()
            logger.debug(f"[Cache:{self.node_id}] HIT key='{key}' state={line.state}")
            return line.value

        # Cache MISS
        self._metrics.record_cache_access(hit=False)
        cache_misses.labels(node_id=self.node_id).inc()
        logger.info(f"[Cache:{self.node_id}] MISS key='{key}', fetching...")

        # Check if any peer has it in Modified state (needs writeback first)
        value, from_peer = await self._fetch_from_peers(key)

        if value is None:
            # Not in any cache — fetch from "memory"
            mem_entry = self._MEMORY.get(key)
            if mem_entry:
                value = mem_entry["value"]
            else:
                return None

        # Notify peers: I now have this in S state
        peer_has_it = from_peer is not None
        initial_state = MESIState.SHARED if peer_has_it else MESIState.EXCLUSIVE

        line = CacheLine(key=key, value=value, state=initial_state)
        evicted = self._lru.put(key, line)
        if evicted:
            await self._handle_eviction(evicted)

        cache_size.labels(node_id=self.node_id).set(len(self._lru))
        return value

    async def _fetch_from_peers(self, key: str) -> tuple[Optional[Any], Optional[str]]:
        """Ask peers if they have the key (handles M-state nodes that need to flush)."""
        tasks = {
            peer: asyncio.create_task(
                self._messenger.send(peer, "/cache/fetch", {"key": key, "requester": self.node_id})
            )
            for peer in self.peers
        }
        for peer, task in tasks.items():
            resp = await task
            if resp and resp.get("found"):
                logger.debug(f"[Cache:{self.node_id}] Fetched '{key}' from peer {peer}")
                return resp["value"], peer
        return None, None

    # ── Write Operation ───────────────────────────────────────────────────────

    async def write(self, key: str, value: Any) -> bool:
        """
        Write a value to the distributed cache.
        MESI write: invalidate all other copies, take Modified state.
        """
        # Step 1: Broadcast INVALIDATE to all peers
        await self._broadcast_invalidate(key)

        # Step 2: Write locally with Modified state
        version = (self._MEMORY.get(key, {}).get("version", 0) + 1)
        line = CacheLine(key=key, value=value, state=MESIState.MODIFIED, version=version)
        evicted = self._lru.put(key, line)
        if evicted:
            await self._handle_eviction(evicted)

        # Step 3: Update "memory" (write-through simulation)
        self._MEMORY[key] = {"value": value, "version": version}

        cache_size.labels(node_id=self.node_id).set(len(self._lru))
        logger.info(f"[Cache:{self.node_id}] WRITE key='{key}' (state=M, version={version})")
        return True

    async def _broadcast_invalidate(self, key: str):
        """Send INVALIDATE message to all peers (MESI write protocol)."""
        tasks = []
        for peer in self.peers:
            tasks.append(
                self._messenger.send(
                    peer, "/cache/invalidate",
                    {"key": key, "invalidator": self.node_id}
                )
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        invalidated = sum(
            1 for r in results if isinstance(r, dict) and r.get("invalidated")
        )
        if invalidated > 0:
            logger.debug(f"[Cache:{self.node_id}] Invalidated '{key}' on {invalidated} peers")

    async def _handle_eviction(self, key: str):
        """Handle LRU eviction: if Modified, writeback to memory."""
        # Note: The key was already evicted from LRU, but we track state
        mem_entry = self._MEMORY.get(key)
        if mem_entry:
            logger.debug(f"[Cache:{self.node_id}] Evicted key='{key}' (writeback to memory)")

    # ── MESI Protocol Handlers (called by HTTP endpoints) ────────────────────

    async def handle_fetch(self, key: str, requester: str) -> dict:
        """
        Another node wants to read this key.
        M → S (writeback + share), E → S, S stays S, I → not found
        """
        line = self._lru.get(key)
        if not line or line.state == MESIState.INVALID:
            return {"found": False}

        if line.state == MESIState.MODIFIED:
            # Writeback to memory before sharing
            self._MEMORY[key] = {"value": line.value, "version": line.version}
            line.state = MESIState.SHARED
            self._lru.update_state(key, MESIState.SHARED)
            logger.info(f"[Cache:{self.node_id}] M→S for '{key}' (writeback + share with {requester})")

        elif line.state == MESIState.EXCLUSIVE:
            line.state = MESIState.SHARED
            self._lru.update_state(key, MESIState.SHARED)
            logger.debug(f"[Cache:{self.node_id}] E→S for '{key}' (sharing with {requester})")

        return {"found": True, "value": line.value, "version": line.version}

    async def handle_invalidate(self, key: str, invalidator: str) -> dict:
        """
        Another node is writing — invalidate our copy.
        Any state → I
        """
        line = self._lru.get(key)
        if line:
            if line.state == MESIState.MODIFIED:
                # Writeback first
                self._MEMORY[key] = {"value": line.value, "version": line.version}
                logger.info(f"[Cache:{self.node_id}] M→I for '{key}' (writeback before invalidate by {invalidator})")
            else:
                logger.debug(f"[Cache:{self.node_id}] {line.state}→I for '{key}' (invalidated by {invalidator})")
            self._lru.invalidate(key)
            cache_invalidations.labels(node_id=self.node_id).inc()
            cache_size.labels(node_id=self.node_id).set(len(self._lru))
            return {"invalidated": True, "key": key}

        return {"invalidated": False, "key": key}

    # ── Admin Operations ──────────────────────────────────────────────────────

    async def delete(self, key: str) -> bool:
        """Delete a key from all caches and memory."""
        await self._broadcast_invalidate(key)
        self._lru.invalidate(key)
        self._MEMORY.pop(key, None)
        return True

    def get_status(self) -> dict:
        lines = []
        for key in self._lru.get_all_keys():
            line = self._lru.get(key)
            if line:
                lines.append(line.to_dict())

        state_counts = {}
        for line_d in lines:
            s = line_d["state"]
            state_counts[s] = state_counts.get(s, 0) + 1

        return {
            "node_id": self.node_id,
            "capacity": self._lru.capacity,
            "size": len(self._lru),
            "utilization": len(self._lru) / self._lru.capacity,
            "state_distribution": state_counts,
            "memory_size": len(self._MEMORY),
            "policy": "LRU",
            "protocol": "MESI",
        }

    def get_cache_lines(self) -> list[dict]:
        """Return all cache lines for inspection."""
        lines = []
        for key in self._lru.get_all_keys():
            line = self._lru.get(key)
            if line:
                lines.append(line.to_dict())
        return lines

    async def start(self):
        self._running = True
        logger.info(f"[Cache:{self.node_id}] MESI Cache started (capacity={self._lru.capacity})")

    async def stop(self):
        self._running = False
        await self._messenger.close()
