"""
Async inter-node message passing menggunakan aiohttp.
Mendukung retry, timeout, dan circuit breaker pattern.
Jika ENABLE_TLS=true, semua request menggunakan HTTPS + JWT auth header.
"""
import asyncio
import json
import time
import logging
from typing import Any, Optional
import aiohttp
import jwt
from src.utils.config import config

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """
    Circuit Breaker untuk menghindari cascade failure.
    States: CLOSED (normal) → OPEN (stop requests) → HALF_OPEN (try again)
    """
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 10.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "CLOSED"

    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"

    def can_attempt(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        return True  # HALF_OPEN: try once


class MessagePassing:
    """
    Async HTTP-based message passing untuk inter-node communication.
    Fitur:
    - Automatic retry dengan exponential backoff
    - Circuit breaker per peer
    - JWT authentication (Bonus D: Security)
    - Timeout handling
    """

    def __init__(self, node_id: str, timeout: float = 2.0, max_retries: int = 3):
        self.node_id = node_id
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit_breakers: dict[str, CircuitBreaker] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    def _get_circuit_breaker(self, peer: str) -> CircuitBreaker:
        if peer not in self._circuit_breakers:
            self._circuit_breakers[peer] = CircuitBreaker()
        return self._circuit_breakers[peer]

    def _make_auth_header(self) -> dict:
        """Generate JWT token for inter-node auth (Bonus D)."""
        payload = {
            "node_id": self.node_id,
            "iat": int(time.time()),
            "exp": int(time.time()) + 3600,
            "role": "node",
        }
        token = jwt.encode(payload, config.SECRET_KEY, algorithm=config.JWT_ALGORITHM)
        return {"Authorization": f"Bearer {token}", "X-Node-ID": self.node_id}

    async def send(
        self,
        peer_url: str,
        endpoint: str,
        data: dict,
        method: str = "POST",
    ) -> Optional[dict]:
        """
        Send a message to a peer node.
        Returns response dict or None on failure.
        """
        cb = self._get_circuit_breaker(peer_url)
        if not cb.can_attempt():
            logger.debug(f"Circuit breaker OPEN for {peer_url}, skipping")
            return None

        url = f"{peer_url}{endpoint}"
        headers = {
            "Content-Type": "application/json",
            **self._make_auth_header(),
        }

        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                async with session.request(
                    method, url, json=data, headers=headers
                ) as resp:
                    if resp.status == 200:
                        cb.record_success()
                        return await resp.json()
                    else:
                        logger.warning(
                            f"Peer {peer_url} returned {resp.status} on {endpoint}"
                        )
                        cb.record_failure()
                        return None
            except asyncio.TimeoutError:
                logger.debug(f"Timeout sending to {peer_url}{endpoint}")
                cb.record_failure()
            except aiohttp.ClientConnectorError:
                logger.debug(f"Connection refused: {peer_url}")
                cb.record_failure()
                break  # Don't retry if node is down
            except Exception as e:
                logger.error(f"Error sending to {peer_url}: {e}")
                cb.record_failure()

            if attempt < self.max_retries - 1:
                await asyncio.sleep(0.05 * (2 ** attempt))  # Exponential backoff

        return None

    async def broadcast(
        self,
        peers: list[str],
        endpoint: str,
        data: dict,
        method: str = "POST",
    ) -> dict[str, Optional[dict]]:
        """
        Broadcast a message to all peers concurrently.
        Returns dict of {peer_url: response}.
        """
        tasks = {
            peer: asyncio.create_task(self.send(peer, endpoint, data, method))
            for peer in peers
        }
        results = {}
        for peer, task in tasks.items():
            results[peer] = await task
        return results

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


def verify_node_token(token: str) -> Optional[dict]:
    """
    Verify JWT token from another node (Bonus D: Security).
    Returns payload if valid, None otherwise.
    """
    try:
        payload = jwt.decode(
            token, config.SECRET_KEY, algorithms=[config.JWT_ALGORITHM]
        )
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Received expired node token")
        return None
    except jwt.InvalidTokenError:
        logger.warning("Received invalid node token")
        return None
