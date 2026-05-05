"""
Practical Byzantine Fault Tolerance (PBFT) - Implementasi Dasar (Bonus A: +5 poin)
Paper: "Practical Byzantine Fault Tolerance" (Castro & Liskov, 1999)

PBFT dapat menoleransi f node Byzantine (malicious) jika total nodes n >= 3f + 1.
3 fases: PRE-PREPARE → PREPARE → COMMIT

Perbedaan dengan Raft:
- Raft: Crash-fault tolerant (node bisa crash/disconnect)
- PBFT: Byzantine-fault tolerant (node bisa mengirim data salah/berbahaya)
"""
import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any
from src.communication.message_passing import MessagePassing
from src.utils.config import config

logger = logging.getLogger(__name__)


class PBFTPhase(str, Enum):
    PRE_PREPARE = "PRE_PREPARE"
    PREPARE = "PREPARE"
    COMMIT = "COMMIT"
    REPLY = "REPLY"


@dataclass
class PBFTMessage:
    phase: str
    view: int
    sequence: int
    digest: str
    node_id: str
    payload: dict
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "phase": self.phase,
            "view": self.view,
            "sequence": self.sequence,
            "digest": self.digest,
            "node_id": self.node_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }


def compute_digest(payload: dict) -> str:
    """Compute SHA-256 digest of a payload for integrity checking."""
    serialized = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(serialized).hexdigest()


class PBFTNode:
    """
    Basic PBFT Node Implementation.
    
    Byzantine fault tolerance:
    - f = (n - 1) // 3  →  max faulty nodes tolerated
    - Need 2f + 1 PREPARE messages to proceed to COMMIT
    - Need 2f + 1 COMMIT messages to execute request
    
    This is a simplified (non-view-change) implementation for educational purposes.
    For production use, view-change protocol and checkpointing are required.
    """

    def __init__(self, node_id: str, peers: list[str], is_byzantine: bool = False):
        self.node_id = node_id
        self.peers = peers
        self.is_byzantine = is_byzantine  # For testing: simulate malicious node
        self._messenger = MessagePassing(node_id)
        self.n = len(peers) + 1  # Total nodes (including self)
        self.f = (self.n - 1) // 3  # Max Byzantine faults

        # PBFT state
        self.view: int = 0
        self.sequence: int = 0
        self.primary_id: str = self._get_primary()

        # Message logs per sequence number
        self._prepare_log: dict[str, list[PBFTMessage]] = {}   # seq → [PREPARE msgs]
        self._commit_log: dict[str, list[PBFTMessage]] = {}    # seq → [COMMIT msgs]
        self._pre_prepare_log: dict[str, PBFTMessage] = {}     # seq → PRE-PREPARE msg
        self._executed: set[str] = set()                        # Executed sequence nums
        self._state_machine: dict[str, Any] = {}
        self._callbacks: list = []

    def _get_primary(self) -> str:
        """Primary = node at index (view % n) in sorted node list."""
        all_nodes = sorted([self.node_id] + self.peers)
        return all_nodes[self.view % self.n]

    @property
    def is_primary(self) -> bool:
        return self.node_id == self.primary_id

    def _quorum(self) -> int:
        """Number of messages needed for quorum: 2f + 1."""
        return 2 * self.f + 1

    # ── Client Request (Primary only) ─────────────────────────────────────────

    async def request(self, command: dict) -> Optional[dict]:
        """
        Submit a request (only primary handles this directly).
        Returns result dict if successful, None otherwise.
        """
        if not self.is_primary:
            logger.warning(f"[PBFT:{self.node_id}] Not primary, forwarding not implemented.")
            return None

        self.sequence += 1
        seq_str = f"{self.view}:{self.sequence}"
        digest = compute_digest(command)

        # PRE-PREPARE: Primary sends to all replicas
        pre_prepare = PBFTMessage(
            phase=PBFTPhase.PRE_PREPARE,
            view=self.view,
            sequence=self.sequence,
            digest=digest,
            node_id=self.node_id,
            payload=command,
        )
        self._pre_prepare_log[seq_str] = pre_prepare
        logger.info(f"[PBFT:{self.node_id}] PRE-PREPARE seq={self.sequence}")

        # Broadcast PRE-PREPARE to all replicas
        broadcast_results = await self._messenger.broadcast(
            self.peers, "/pbft/pre-prepare", pre_prepare.to_dict()
        )

        # Wait for PREPARE quorum (simulate with async wait)
        await asyncio.sleep(0.1)  # Allow replicas to respond

        prepare_count = sum(1 for r in broadcast_results.values() if r is not None)
        logger.info(f"[PBFT:{self.node_id}] Received {prepare_count} PREPARE acks")

        if prepare_count + 1 >= self._quorum():  # +1 for self
            # COMMIT phase
            commit_msg = PBFTMessage(
                phase=PBFTPhase.COMMIT,
                view=self.view,
                sequence=self.sequence,
                digest=digest,
                node_id=self.node_id,
                payload=command,
            )
            commit_results = await self._messenger.broadcast(
                self.peers, "/pbft/commit", commit_msg.to_dict()
            )
            await asyncio.sleep(0.05)
            commit_count = sum(1 for r in commit_results.values() if r is not None)

            if commit_count + 1 >= self._quorum():
                result = await self._execute(command, self.sequence)
                logger.info(f"[PBFT:{self.node_id}] ✅ Request executed at seq={self.sequence}")
                return result

        logger.warning(f"[PBFT:{self.node_id}] Failed to reach quorum for seq={self.sequence}")
        return None

    # ── RPC Handlers ──────────────────────────────────────────────────────────

    async def handle_pre_prepare(self, msg: dict) -> dict:
        """Handle PRE-PREPARE from primary."""
        if self.is_byzantine:
            # Byzantine node: send fake response
            logger.warning(f"[PBFT:{self.node_id}] 👿 Byzantine: sending fake prepare")
            return {"phase": "PREPARE", "accepted": False, "node_id": self.node_id}

        view = msg["view"]
        sequence = msg["sequence"]
        digest = msg["digest"]
        payload = msg["payload"]

        seq_str = f"{view}:{sequence}"

        # Validate
        computed = compute_digest(payload)
        if computed != digest:
            logger.warning(f"[PBFT:{self.node_id}] Digest mismatch! Rejecting PRE-PREPARE")
            return {"phase": "PREPARE", "accepted": False, "node_id": self.node_id}

        if seq_str in self._pre_prepare_log:
            logger.warning(f"[PBFT:{self.node_id}] Duplicate PRE-PREPARE for {seq_str}")
            return {"phase": "PREPARE", "accepted": False, "node_id": self.node_id}

        self._pre_prepare_log[seq_str] = PBFTMessage(
            phase=PBFTPhase.PRE_PREPARE, view=view, sequence=sequence,
            digest=digest, node_id=msg["node_id"], payload=payload
        )

        # Send PREPARE to all peers
        prepare_msg = PBFTMessage(
            phase=PBFTPhase.PREPARE, view=view, sequence=sequence,
            digest=digest, node_id=self.node_id, payload={}
        )
        asyncio.create_task(
            self._messenger.broadcast(self.peers, "/pbft/prepare", prepare_msg.to_dict())
        )

        logger.info(f"[PBFT:{self.node_id}] Accepted PRE-PREPARE, sent PREPARE (seq={sequence})")
        return {"phase": "PREPARE", "accepted": True, "node_id": self.node_id}

    async def handle_prepare(self, msg: dict) -> dict:
        """Handle PREPARE message from a replica."""
        seq_str = f"{msg['view']}:{msg['sequence']}"
        if seq_str not in self._prepare_log:
            self._prepare_log[seq_str] = []

        pm = PBFTMessage(
            phase=PBFTPhase.PREPARE, view=msg["view"], sequence=msg["sequence"],
            digest=msg["digest"], node_id=msg["node_id"], payload={}
        )

        # Deduplicate
        existing_nodes = {m.node_id for m in self._prepare_log[seq_str]}
        if msg["node_id"] not in existing_nodes:
            self._prepare_log[seq_str].append(pm)

        count = len(self._prepare_log[seq_str])
        logger.debug(f"[PBFT:{self.node_id}] PREPARE count for {seq_str}: {count}/{self._quorum()}")
        return {"accepted": True, "node_id": self.node_id, "prepare_count": count}

    async def handle_commit(self, msg: dict) -> dict:
        """Handle COMMIT message from a replica."""
        seq_str = f"{msg['view']}:{msg['sequence']}"
        if seq_str not in self._commit_log:
            self._commit_log[seq_str] = []

        cm = PBFTMessage(
            phase=PBFTPhase.COMMIT, view=msg["view"], sequence=msg["sequence"],
            digest=msg["digest"], node_id=msg["node_id"], payload=msg.get("payload", {})
        )
        existing_nodes = {m.node_id for m in self._commit_log[seq_str]}
        if msg["node_id"] not in existing_nodes:
            self._commit_log[seq_str].append(cm)

        # If we have quorum and haven't executed
        commit_count = len(self._commit_log[seq_str])
        if commit_count >= self._quorum() and seq_str not in self._executed:
            pre_prepare = self._pre_prepare_log.get(seq_str)
            if pre_prepare:
                await self._execute(pre_prepare.payload, msg["sequence"])

        return {"accepted": True, "node_id": self.node_id, "commit_count": commit_count}

    async def _execute(self, command: dict, sequence: int) -> dict:
        """Execute a committed command on the state machine."""
        seq_str = f"{self.view}:{sequence}"
        if seq_str in self._executed:
            return {"status": "already_executed"}
        self._executed.add(seq_str)

        # Apply to state machine
        result = {"executed": True, "sequence": sequence, "command": command}
        self._state_machine[seq_str] = result

        for cb in self._callbacks:
            try:
                await cb(command, result)
            except Exception as e:
                logger.error(f"[PBFT:{self.node_id}] Callback error: {e}")

        logger.info(f"[PBFT:{self.node_id}] Executed command at seq={sequence}")
        return result

    def on_commit(self, callback):
        self._callbacks.append(callback)

    def get_status(self) -> dict:
        return {
            "node_id": self.node_id,
            "is_primary": self.is_primary,
            "primary_id": self.primary_id,
            "view": self.view,
            "sequence": self.sequence,
            "total_nodes": self.n,
            "max_byzantine_faults": self.f,
            "quorum_size": self._quorum(),
            "executed_count": len(self._executed),
            "is_byzantine": self.is_byzantine,
        }
