"""
Distributed Queue System dengan Consistent Hashing.
Fitur:
- Multiple producers dan consumers
- Consistent hashing untuk distribusi messages ke nodes
- Message persistence via Redis
- At-least-once delivery guarantee
- Node failure recovery (messages tidak hilang)
"""
import asyncio
import hashlib
import json
import time
import uuid
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, Callable
from sortedcontainers import SortedList
import redis.asyncio as aioredis
from src.communication.message_passing import MessagePassing
from src.utils.config import config
from src.utils.metrics import (
    queue_messages_produced, queue_messages_consumed,
    queue_messages_pending, queue_delivery_latency
)

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A queue message with delivery tracking."""
    message_id: str
    queue_name: str
    payload: dict
    producer_id: str
    created_at: float = field(default_factory=time.time)
    attempts: int = 0
    max_attempts: int = 3

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(**d)

    @property
    def is_exhausted(self) -> bool:
        return self.attempts >= self.max_attempts


class ConsistentHashRing:
    """
    Consistent Hash Ring untuk distribusi load ke nodes.
    
    Menggunakan virtual nodes (vnodes) untuk distribusi yang lebih merata.
    Virtual nodes membantu saat ada node yang ditambah/dihapus (minimal remapping).
    """

    def __init__(self, vnodes: int = 150):
        self.vnodes = vnodes
        self._ring: SortedList = SortedList()  # Sorted list of (hash_value, node_url)
        self._node_map: dict[int, str] = {}    # hash_value → node_url

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_url: str):
        """Add a node with virtual replicas."""
        for i in range(self.vnodes):
            vnode_key = f"{node_url}:vnode:{i}"
            h = self._hash(vnode_key)
            self._ring.add(h)
            self._node_map[h] = node_url
        logger.info(f"[HashRing] Added node {node_url} with {self.vnodes} vnodes")

    def remove_node(self, node_url: str):
        """Remove a node and its virtual replicas."""
        for i in range(self.vnodes):
            vnode_key = f"{node_url}:vnode:{i}"
            h = self._hash(vnode_key)
            if h in self._node_map:
                self._ring.remove(h)
                del self._node_map[h]
        logger.info(f"[HashRing] Removed node {node_url}")

    def get_node(self, key: str) -> Optional[str]:
        """Get the responsible node for a given key."""
        if not self._ring:
            return None
        h = self._hash(key)
        # Find first node clockwise
        idx = self._ring.bisect_right(h)
        if idx >= len(self._ring):
            idx = 0  # Wrap around
        return self._node_map[self._ring[idx]]

    def get_nodes_for_key(self, key: str, n: int = 3) -> list[str]:
        """Get n nodes responsible for a key (for replication)."""
        if not self._ring:
            return []
        h = self._hash(key)
        idx = self._ring.bisect_right(h)
        nodes = []
        seen = set()
        for i in range(len(self._ring)):
            pos = (idx + i) % len(self._ring)
            node = self._node_map[self._ring[pos]]
            if node not in seen:
                nodes.append(node)
                seen.add(node)
            if len(nodes) == n:
                break
        return nodes

    def get_all_nodes(self) -> list[str]:
        return list(set(self._node_map.values()))


class DistributedQueue:
    """
    Distributed Queue System.
    
    Architecture:
    - Messages diroute ke node berdasarkan consistent hashing (queue_name sebagai key)
    - Redis digunakan untuk persistence (at-least-once delivery)
    - Acknowledgment mechanism untuk delivery guarantee
    - Automatic retry untuk failed deliveries
    """

    def __init__(self, node_id: str, node_url: str, peers: list[str]):
        self.node_id = node_id
        self.node_url = node_url
        self.peers = peers
        self._messenger = MessagePassing(node_id)
        self._redis: Optional[aioredis.Redis] = None
        self._hash_ring = ConsistentHashRing()
        self._consumers: dict[str, list[Callable]] = {}  # queue_name → callbacks
        self._running = False
        self._poll_task: Optional[asyncio.Task] = None

        # Initialize hash ring with all nodes
        self._hash_ring.add_node(node_url)
        for peer in peers:
            self._hash_ring.add_node(peer)

    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = await aioredis.from_url(
                config.redis_url, decode_responses=True, max_connections=10
            )
        return self._redis

    # ── Produce ───────────────────────────────────────────────────────────────

    async def produce(self, queue_name: str, payload: dict, producer_id: str = "default") -> str:
        """
        Produce a message to a queue.
        Routes to responsible node via consistent hashing.
        Returns message_id.
        """
        msg = Message(
            message_id=str(uuid.uuid4()),
            queue_name=queue_name,
            payload=payload,
            producer_id=producer_id,
        )

        # Determine responsible node
        responsible_node = self._hash_ring.get_node(queue_name)
        replication_nodes = self._hash_ring.get_nodes_for_key(queue_name, n=2)

        if responsible_node == self.node_url:
            # We are responsible: persist and enqueue
            await self._persist_message(msg)
            logger.info(f"[Queue:{self.node_id}] Produced msg {msg.message_id} to '{queue_name}'")
        else:
            # Forward to responsible node
            resp = await self._messenger.send(
                responsible_node, "/queue/produce",
                {"queue_name": queue_name, "payload": payload, "producer_id": producer_id}
            )
            if resp is None:
                # Fallback: store locally if remote node is down
                logger.warning(f"[Queue:{self.node_id}] Primary node down, storing locally")
                await self._persist_message(msg)

        # Replicate to backup nodes for fault tolerance
        for backup in replication_nodes:
            if backup != responsible_node and backup != self.node_url:
                asyncio.create_task(
                    self._messenger.send(
                        backup, "/queue/replicate",
                        {"message": msg.to_dict()}
                    )
                )

        queue_messages_produced.labels(
            node_id=self.node_id, queue_name=queue_name
        ).inc()

        return msg.message_id

    async def _persist_message(self, msg: Message):
        """Persist message to Redis for durability."""
        r = await self._get_redis()
        queue_key = f"queue:{msg.queue_name}:pending"
        msg_key = f"msg:{msg.message_id}"

        pipe = r.pipeline()
        pipe.set(msg_key, json.dumps(msg.to_dict()), ex=86400)  # 24h TTL
        pipe.lpush(queue_key, msg.message_id)
        await pipe.execute()

        pending = await r.llen(queue_key)
        queue_messages_pending.labels(
            node_id=self.node_id, queue_name=msg.queue_name
        ).set(pending)

    # ── Consume ───────────────────────────────────────────────────────────────

    async def consume(self, queue_name: str, consumer_id: str, timeout: float = 5.0) -> Optional[dict]:
        """
        Consume a single message from a queue (blocking with timeout).
        Returns message dict or None if timeout.
        """
        r = await self._get_redis()
        queue_key = f"queue:{queue_name}:pending"
        processing_key = f"queue:{queue_name}:processing:{consumer_id}"

        # BRPOPLPUSH: atomic move from pending to processing (at-least-once guarantee)
        try:
            msg_id = await r.brpoplpush(queue_key, processing_key, timeout=timeout)
        except Exception:
            msg_id = None

        if not msg_id:
            return None

        msg_data = await r.get(f"msg:{msg_id}")
        if not msg_data:
            return None

        msg = Message.from_dict(json.loads(msg_data))
        msg.attempts += 1
        await r.set(f"msg:{msg.message_id}", json.dumps(msg.to_dict()), ex=86400)

        produce_time = msg.created_at
        queue_delivery_latency.labels(queue_name=queue_name).observe(
            time.time() - produce_time
        )
        queue_messages_consumed.labels(
            node_id=self.node_id, queue_name=queue_name
        ).inc()

        logger.info(f"[Queue:{self.node_id}] Consumer {consumer_id} consuming msg {msg.message_id}")
        return msg.to_dict()

    async def acknowledge(self, queue_name: str, message_id: str, consumer_id: str) -> bool:
        """
        Acknowledge successful processing of a message.
        Removes from processing queue and deletes the message.
        """
        r = await self._get_redis()
        processing_key = f"queue:{queue_name}:processing:{consumer_id}"

        # Remove from processing
        await r.lrem(processing_key, 0, message_id)
        await r.delete(f"msg:{message_id}")

        pending = await r.llen(f"queue:{queue_name}:pending")
        queue_messages_pending.labels(
            node_id=self.node_id, queue_name=queue_name
        ).set(pending)

        logger.info(f"[Queue:{self.node_id}] ACK msg {message_id}")
        return True

    async def nack(self, queue_name: str, message_id: str, consumer_id: str) -> bool:
        """
        Negative acknowledge: put message back to queue for retry.
        If max attempts exceeded, move to dead letter queue.
        """
        r = await self._get_redis()
        processing_key = f"queue:{queue_name}:processing:{consumer_id}"
        msg_data = await r.get(f"msg:{message_id}")

        if not msg_data:
            return False

        msg = Message.from_dict(json.loads(msg_data))
        await r.lrem(processing_key, 0, message_id)

        if msg.is_exhausted:
            # Move to dead letter queue
            dlq_key = f"queue:{queue_name}:dlq"
            await r.lpush(dlq_key, message_id)
            logger.warning(f"[Queue:{self.node_id}] Msg {message_id} moved to DLQ after {msg.attempts} attempts")
        else:
            # Re-enqueue
            await r.lpush(f"queue:{queue_name}:pending", message_id)
            logger.info(f"[Queue:{self.node_id}] Msg {message_id} re-queued (attempt {msg.attempts})")

        return True

    # ── Recovery ──────────────────────────────────────────────────────────────

    async def recover_processing(self, consumer_id: str, queue_name: str):
        """
        Recovery: Re-enqueue messages stuck in processing state.
        Called on consumer restart to handle node failures.
        """
        r = await self._get_redis()
        processing_key = f"queue:{queue_name}:processing:{consumer_id}"
        pending_key = f"queue:{queue_name}:pending"

        stuck_messages = await r.lrange(processing_key, 0, -1)
        if stuck_messages:
            logger.info(f"[Queue:{self.node_id}] Recovering {len(stuck_messages)} stuck messages for {consumer_id}")
            for msg_id in stuck_messages:
                await r.lmove(processing_key, pending_key, "RIGHT", "LEFT")

    def register_consumer(self, queue_name: str, callback: Callable):
        """Register an async callback to be called when messages arrive."""
        if queue_name not in self._consumers:
            self._consumers[queue_name] = []
        self._consumers[queue_name].append(callback)

    async def handle_node_failure(self, failed_node: str):
        """Handle a peer node failure - take over its queues."""
        logger.warning(f"[Queue:{self.node_id}] Peer {failed_node} failed, checking queue ownership")
        self._hash_ring.remove_node(failed_node)
        # The consistent hash ring will naturally reroute to next available nodes

    async def get_queue_stats(self, queue_name: str) -> dict:
        """Get queue statistics."""
        r = await self._get_redis()
        pending = await r.llen(f"queue:{queue_name}:pending")
        dlq = await r.llen(f"queue:{queue_name}:dlq")
        return {
            "queue_name": queue_name,
            "pending": pending,
            "dead_letter": dlq,
            "responsible_node": self._hash_ring.get_node(queue_name),
            "replication_nodes": self._hash_ring.get_nodes_for_key(queue_name),
        }

    def get_ring_info(self) -> dict:
        return {
            "nodes": self._hash_ring.get_all_nodes(),
            "vnodes_per_node": self._hash_ring.vnodes,
        }

    async def start(self):
        self._running = True
        logger.info(f"[Queue:{self.node_id}] Distributed Queue started")

    async def stop(self):
        self._running = False
        if self._redis:
            await self._redis.aclose()
