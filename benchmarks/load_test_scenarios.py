"""
Load Testing Scenarios menggunakan Locust.
Menguji throughput, latency, dan scalability dari semua komponen.

Cara menjalankan:
    locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8001

Atau headless (untuk CI/automated benchmarks):
    locust -f benchmarks/load_test_scenarios.py \
        --host=http://localhost:8001 \
        --headless -u 50 -r 10 --run-time 60s \
        --csv=benchmarks/results/load_test
"""
import random
import string
import time
import json
from locust import HttpUser, task, between, events, constant_throughput
from locust.env import Environment


def random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def random_resource() -> str:
    resources = [f"resource-{i}" for i in range(20)]
    return random.choice(resources)


# ── Lock Manager Load Test ────────────────────────────────────────────────────

class LockManagerUser(HttpUser):
    """
    Simulates concurrent clients acquiring and releasing locks.
    Target: measure lock throughput and deadlock detection under load.
    """
    wait_time = between(0.1, 0.5)
    weight = 3

    def on_start(self):
        self.headers = {"Content-Type": "application/json"}
        self.held_locks = []

    @task(5)
    def acquire_shared_lock(self):
        resource = random_resource()
        owner = f"reader-{random_string(4)}"
        with self.client.post(
            "/lock/acquire",
            json={"resource": resource, "lock_type": "shared", "owner": owner, "wait": False},
            headers=self.headers,
            catch_response=True,
            name="/lock/acquire [shared]"
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "granted" and data.get("lock_id"):
                    self.held_locks.append((data["lock_id"], owner))
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(3)
    def acquire_exclusive_lock(self):
        resource = random_resource()
        owner = f"writer-{random_string(4)}"
        with self.client.post(
            "/lock/acquire",
            json={"resource": resource, "lock_type": "exclusive", "owner": owner, "wait": False},
            headers=self.headers,
            catch_response=True,
            name="/lock/acquire [exclusive]"
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "granted" and data.get("lock_id"):
                    self.held_locks.append((data["lock_id"], owner))
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(4)
    def release_lock(self):
        if not self.held_locks:
            return
        lock_id, owner = self.held_locks.pop(0)
        with self.client.post(
            "/lock/release",
            json={"lock_id": lock_id, "owner": owner},
            headers=self.headers,
            catch_response=True,
            name="/lock/release"
        ) as resp:
            resp.success() if resp.status_code == 200 else resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def check_lock_status(self):
        self.client.get("/lock/status", name="/lock/status")

    def on_stop(self):
        # Release all held locks on stop
        for lock_id, owner in self.held_locks:
            self.client.post("/lock/release", json={"lock_id": lock_id, "owner": owner})


# ── Queue Load Test ───────────────────────────────────────────────────────────

class QueueProducerUser(HttpUser):
    """High-throughput message producer."""
    wait_time = between(0.05, 0.2)
    weight = 2

    def on_start(self):
        self.headers = {"Content-Type": "application/json"}
        self.queue_names = [f"queue-{chr(ord('a') + i)}" for i in range(5)]
        self.produced_count = 0

    @task(10)
    def produce_message(self):
        queue_name = random.choice(self.queue_names)
        with self.client.post(
            "/queue/produce",
            json={
                "queue_name": queue_name,
                "payload": {
                    "event": "order_placed",
                    "amount": round(random.uniform(10, 1000), 2),
                    "product_id": random.randint(1, 1000),
                    "timestamp": time.time(),
                },
                "producer_id": f"producer-{random_string(4)}"
            },
            headers=self.headers,
            catch_response=True,
            name="/queue/produce"
        ) as resp:
            if resp.status_code == 200:
                self.produced_count += 1
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def check_queue_stats(self):
        queue_name = random.choice(self.queue_names)
        self.client.get(f"/queue/stats?queue_name={queue_name}", name="/queue/stats")


class QueueConsumerUser(HttpUser):
    """Concurrent message consumer with ack/nack."""
    wait_time = between(0.1, 0.3)
    weight = 2

    def on_start(self):
        self.headers = {"Content-Type": "application/json"}
        self.queue_names = [f"queue-{chr(ord('a') + i)}" for i in range(5)]
        self.consumer_id = f"consumer-{random_string(6)}"

    @task(8)
    def consume_and_ack(self):
        queue_name = random.choice(self.queue_names)
        with self.client.get(
            f"/queue/consume?queue_name={queue_name}&consumer_id={self.consumer_id}&timeout=0.5",
            headers=self.headers,
            catch_response=True,
            name="/queue/consume"
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                msg = data.get("message")
                if msg:
                    # Acknowledge
                    self.client.post(
                        "/queue/ack",
                        json={
                            "queue_name": queue_name,
                            "message_id": msg["message_id"],
                            "consumer_id": self.consumer_id
                        },
                        headers=self.headers,
                        name="/queue/ack"
                    )
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(2)
    def consume_and_nack(self):
        queue_name = random.choice(self.queue_names)
        with self.client.get(
            f"/queue/consume?queue_name={queue_name}&consumer_id={self.consumer_id}&timeout=0.3",
            headers=self.headers,
            catch_response=True,
            name="/queue/consume [nack]"
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                msg = data.get("message")
                if msg:
                    self.client.post(
                        "/queue/nack",
                        json={
                            "queue_name": queue_name,
                            "message_id": msg["message_id"],
                            "consumer_id": self.consumer_id
                        },
                        headers=self.headers,
                        name="/queue/nack"
                    )
                resp.success()


# ── Cache Load Test ───────────────────────────────────────────────────────────

class CacheUser(HttpUser):
    """Mixed read/write cache operations to measure hit ratio and latency."""
    wait_time = between(0.01, 0.1)
    weight = 4

    def on_start(self):
        self.headers = {"Content-Type": "application/json"}
        # Pre-populate some keys
        self.hot_keys = [f"hot-key-{i}" for i in range(20)]
        self.cold_keys = [f"cold-key-{random_string(8)}" for _ in range(50)]

    @task(7)
    def read_hot_key(self):
        key = random.choice(self.hot_keys)
        self.client.get(f"/cache/read?key={key}", name="/cache/read [hot]")

    @task(3)
    def read_cold_key(self):
        key = random.choice(self.cold_keys)
        self.client.get(f"/cache/read?key={key}", name="/cache/read [cold]")

    @task(2)
    def write_hot_key(self):
        key = random.choice(self.hot_keys)
        self.client.post(
            "/cache/write",
            json={"key": key, "value": {"data": random_string(20), "ts": time.time()}},
            headers=self.headers,
            name="/cache/write [hot]"
        )

    @task(1)
    def cache_status(self):
        self.client.get("/cache/status", name="/cache/status")


# ── Raft / Cluster Health ─────────────────────────────────────────────────────

class ClusterHealthUser(HttpUser):
    """Monitor cluster health and Raft consensus state."""
    wait_time = between(0.5, 2.0)
    weight = 1

    @task(5)
    def check_health(self):
        self.client.get("/health", name="/health")

    @task(3)
    def check_status(self):
        self.client.get("/status", name="/status")

    @task(2)
    def check_raft(self):
        self.client.get("/raft/status", name="/raft/status")


# ── Event Hooks for Custom Reporting ─────────────────────────────────────────

@events.test_start.add_listener
def on_test_start(environment: Environment, **kwargs):
    print("\n" + "="*60)
    print("  Distributed Sync System - Load Test Started")
    print("="*60)
    print(f"  Target: {environment.host}")
    print(f"  Components: Lock Manager, Queue, Cache, Raft")
    print("="*60 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment: Environment, **kwargs):
    print("\n" + "="*60)
    print("  Load Test Complete")
    print("="*60)
    stats = environment.runner.stats
    print(f"  Total requests: {stats.total.num_requests}")
    print(f"  Total failures: {stats.total.num_failures}")
    print(f"  Avg response time: {stats.total.avg_response_time:.1f}ms")
    print(f"  RPS: {stats.total.current_rps:.1f}")
    print("="*60 + "\n")
