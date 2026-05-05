"""
Metrics collection menggunakan Prometheus.
Tracks: lock operations, queue throughput, cache hit/miss, node status.
"""
import time
import functools
from prometheus_client import Counter, Histogram, Gauge, Summary, start_http_server
from src.utils.config import config

# ── Lock Manager Metrics ──────────────────────────────────────────────────────
lock_requests_total = Counter(
    "lock_requests_total",
    "Total lock requests",
    ["node_id", "lock_type", "status"],
)
lock_duration_seconds = Histogram(
    "lock_duration_seconds",
    "Time a lock is held",
    ["node_id", "lock_type"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 10],
)
active_locks = Gauge("active_locks", "Currently active locks", ["node_id", "lock_type"])
deadlock_detected_total = Counter(
    "deadlock_detected_total", "Total deadlocks detected", ["node_id"]
)

# ── Queue Metrics ─────────────────────────────────────────────────────────────
queue_messages_produced = Counter(
    "queue_messages_produced_total", "Messages produced", ["node_id", "queue_name"]
)
queue_messages_consumed = Counter(
    "queue_messages_consumed_total", "Messages consumed", ["node_id", "queue_name"]
)
queue_messages_pending = Gauge(
    "queue_messages_pending", "Messages waiting in queue", ["node_id", "queue_name"]
)
queue_delivery_latency = Histogram(
    "queue_delivery_latency_seconds",
    "Latency from produce to consume",
    ["queue_name"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1],
)

# ── Cache Metrics ─────────────────────────────────────────────────────────────
cache_hits = Counter("cache_hits_total", "Cache hits", ["node_id"])
cache_misses = Counter("cache_misses_total", "Cache misses", ["node_id"])
cache_invalidations = Counter(
    "cache_invalidations_total", "Cache invalidations", ["node_id"]
)
cache_size = Gauge("cache_size", "Current cache size in entries", ["node_id"])
cache_hit_ratio = Gauge("cache_hit_ratio", "Cache hit ratio (0-1)", ["node_id"])

# ── Raft Metrics ──────────────────────────────────────────────────────────────
raft_term = Gauge("raft_term", "Current Raft term", ["node_id"])
raft_role = Gauge(
    "raft_role",
    "Raft role: 0=follower, 1=candidate, 2=leader",
    ["node_id"],
)
raft_log_size = Gauge("raft_log_size", "Raft log entries count", ["node_id"])
raft_elections_total = Counter(
    "raft_elections_total", "Total elections started", ["node_id"]
)

# ── Node Health ───────────────────────────────────────────────────────────────
node_up = Gauge("node_up", "Node is alive", ["node_id"])
request_latency = Summary(
    "request_latency_seconds", "API request latency", ["node_id", "endpoint"]
)


class MetricsTracker:
    """Helper class to track metrics with context managers."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._cache_hits = 0
        self._cache_total = 0
        node_up.labels(node_id=node_id).set(1)

    def record_cache_access(self, hit: bool):
        self._cache_total += 1
        if hit:
            self._cache_hits += 1
            cache_hits.labels(node_id=self.node_id).inc()
        else:
            cache_misses.labels(node_id=self.node_id).inc()
        ratio = self._cache_hits / self._cache_total if self._cache_total else 0
        cache_hit_ratio.labels(node_id=self.node_id).set(ratio)

    def update_raft_state(self, term: int, role: str, log_size: int):
        role_map = {"follower": 0, "candidate": 1, "leader": 2}
        raft_term.labels(node_id=self.node_id).set(term)
        raft_role.labels(node_id=self.node_id).set(role_map.get(role, 0))
        raft_log_size.labels(node_id=self.node_id).set(log_size)


def start_metrics_server():
    """Start Prometheus metrics HTTP server."""
    if config.ENABLE_METRICS:
        try:
            start_http_server(config.METRICS_PORT)
            print(f"[Metrics] Prometheus server on port {config.METRICS_PORT}")
        except OSError:
            print(f"[Metrics] Port {config.METRICS_PORT} already in use, skipping.")


def track_latency(endpoint: str):
    """Decorator to track endpoint latency."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            result = await func(*args, **kwargs)
            elapsed = time.time() - start
            request_latency.labels(
                node_id=config.NODE_ID, endpoint=endpoint
            ).observe(elapsed)
            return result
        return wrapper
    return decorator
