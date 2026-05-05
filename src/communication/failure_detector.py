"""
Failure Detector menggunakan Phi Accrual Algorithm (seperti yang dipakai Cassandra & Akka).
Lebih akurat daripada simple timeout karena adaptif terhadap network conditions.
"""
import asyncio
import time
import math
import logging
from collections import deque
from typing import Optional
from src.communication.message_passing import MessagePassing
from src.utils.config import config

logger = logging.getLogger(__name__)


class PhiAccrualDetector:
    """
    Phi Accrual Failure Detector.
    
    Phi threshold:
    - phi < 1   : node sangat likely alive
    - phi > 8   : node sangat likely dead (configurable threshold)
    - phi ~ 4-6 : node mungkin bermasalah
    """

    def __init__(self, window_size: int = 10, phi_threshold: float = 8.0):
        self._heartbeat_times: deque[float] = deque(maxlen=window_size)
        self._last_heartbeat: Optional[float] = None
        self.phi_threshold = phi_threshold

    def heartbeat(self):
        """Record a received heartbeat."""
        now = time.time()
        if self._last_heartbeat is not None:
            interval = now - self._last_heartbeat
            self._heartbeat_times.append(interval)
        self._last_heartbeat = now

    def phi(self) -> float:
        """Compute the phi value (suspicion level)."""
        if not self._heartbeat_times or self._last_heartbeat is None:
            return 0.0
        elapsed = time.time() - self._last_heartbeat
        if len(self._heartbeat_times) < 2:
            return 0.0
        mean = sum(self._heartbeat_times) / len(self._heartbeat_times)
        variance = sum((x - mean) ** 2 for x in self._heartbeat_times) / len(
            self._heartbeat_times
        )
        std = math.sqrt(variance) if variance > 0 else 0.001
        # CDF of normal distribution approximation
        y = (elapsed - mean) / std
        e = math.exp(-1.7075711 * (y + 0.8509636))
        cdf = 1.0 / (1.0 + e)
        phi = -math.log10(max(1.0 - cdf, 1e-15))
        return phi

    @property
    def is_alive(self) -> bool:
        return self.phi() < self.phi_threshold

    @property
    def has_heartbeat(self) -> bool:
        return self._last_heartbeat is not None


class FailureDetector:
    """
    Monitors all peer nodes and tracks their health status.
    Runs heartbeat checks in the background.
    """

    def __init__(self, node_id: str, peers: list[str]):
        self.node_id = node_id
        self.peers = peers
        self._detectors: dict[str, PhiAccrualDetector] = {
            peer: PhiAccrualDetector() for peer in peers
        }
        self._messenger = MessagePassing(node_id)
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._callbacks: list = []

    def on_peer_failure(self, callback):
        """Register callback when a peer is detected as failed."""
        self._callbacks.append(callback)

    def get_live_peers(self) -> list[str]:
        return [
            peer
            for peer, det in self._detectors.items()
            if det.is_alive and det.has_heartbeat
        ]

    def get_dead_peers(self) -> list[str]:
        return [
            peer
            for peer, det in self._detectors.items()
            if not det.is_alive and det.has_heartbeat
        ]

    def record_heartbeat(self, peer: str):
        """Called when we receive a heartbeat (or successful response) from peer."""
        if peer in self._detectors:
            self._detectors[peer].heartbeat()

    def get_peer_status(self) -> dict[str, dict]:
        return {
            peer: {
                "alive": det.is_alive,
                "phi": round(det.phi(), 3),
                "has_contact": det.has_heartbeat,
            }
            for peer, det in self._detectors.items()
        }

    async def _probe_peers(self):
        """Probe all peers with a ping."""
        for peer in self.peers:
            resp = await self._messenger.send(peer, "/health", {}, method="GET")
            if resp is not None:
                self.record_heartbeat(peer)
            else:
                # Check if transitioned to dead
                det = self._detectors[peer]
                if det.has_heartbeat and not det.is_alive:
                    logger.warning(f"[FailureDetector] Peer {peer} FAILED (phi={det.phi():.2f})")
                    for cb in self._callbacks:
                        asyncio.create_task(cb(peer))

    async def _run(self):
        interval = config.RAFT_HEARTBEAT_INTERVAL / 1000  # Convert ms to seconds
        while self._running:
            await self._probe_peers()
            await asyncio.sleep(interval * 2)  # Probe every 2x heartbeat

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(f"[FailureDetector] Started monitoring {len(self.peers)} peers")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        await self._messenger.close()
