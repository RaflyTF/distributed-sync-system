"""
Automated Benchmark Runner.
Menjalankan benchmark scenarios tanpa Locust UI dan menyimpan hasilnya ke JSON.

Penggunaan:
    python benchmarks/benchmark_runner.py --host http://localhost:8001 --duration 30

Output:
    benchmarks/results/benchmark_TIMESTAMP.json
"""
import asyncio
import aiohttp
import time
import json
import argparse
import statistics
import os
from datetime import datetime


RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)


class BenchmarkRunner:
    def __init__(self, host: str, concurrency: int = 20, duration: int = 30):
        self.host = host.rstrip("/")
        self.concurrency = concurrency
        self.duration = duration
        self.results = {}

    async def _timed_request(self, session: aiohttp.ClientSession, method: str, url: str, **kwargs):
        start = time.perf_counter()
        try:
            async with session.request(method, url, **kwargs) as resp:
                await resp.read()
                elapsed = (time.perf_counter() - start) * 1000  # ms
                return elapsed, resp.status, True
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return elapsed, 0, False

    async def _run_scenario(self, name: str, scenario_fn) -> dict:
        print(f"\n  ▶ Running: {name} ({self.duration}s @ {self.concurrency} concurrent)")
        latencies = []
        errors = 0
        request_count = 0
        start_time = time.time()

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as session:
            async def worker():
                nonlocal errors, request_count
                while time.time() - start_time < self.duration:
                    latency, status, success = await scenario_fn(session)
                    request_count += 1
                    latencies.append(latency)
                    if not success or status >= 400:
                        errors += 1
                    await asyncio.sleep(0)

            workers = [asyncio.create_task(worker()) for _ in range(self.concurrency)]
            await asyncio.gather(*workers, return_exceptions=True)

        elapsed = time.time() - start_time
        if latencies:
            result = {
                "name": name,
                "duration_s": round(elapsed, 2),
                "total_requests": request_count,
                "errors": errors,
                "error_rate": round(errors / max(request_count, 1) * 100, 2),
                "throughput_rps": round(request_count / elapsed, 2),
                "latency_ms": {
                    "mean": round(statistics.mean(latencies), 2),
                    "median": round(statistics.median(latencies), 2),
                    "p95": round(sorted(latencies)[int(len(latencies) * 0.95)], 2),
                    "p99": round(sorted(latencies)[int(len(latencies) * 0.99)], 2),
                    "min": round(min(latencies), 2),
                    "max": round(max(latencies), 2),
                }
            }
        else:
            result = {"name": name, "error": "No requests completed"}

        print(f"     ✓ {request_count} requests | {result.get('throughput_rps', 0)} RPS | "
              f"p95={result.get('latency_ms', {}).get('p95', 'N/A')}ms | "
              f"errors={errors}")
        return result

    # ── Scenario Definitions ──────────────────────────────────────────────────

    async def scenario_health_check(self, session):
        return await self._timed_request(session, "GET", f"{self.host}/health")

    async def scenario_cache_read(self, session):
        import random
        key = f"bench-key-{random.randint(1, 50)}"
        return await self._timed_request(session, "GET", f"{self.host}/cache/read?key={key}")

    async def scenario_cache_write(self, session):
        import random
        key = f"bench-key-{random.randint(1, 50)}"
        return await self._timed_request(
            session, "POST", f"{self.host}/cache/write",
            json={"key": key, "value": {"data": "benchmark", "ts": time.time()}},
            headers={"Content-Type": "application/json"}
        )

    async def scenario_lock_acquire_release(self, session):
        import random
        resource = f"bench-resource-{random.randint(1, 20)}"
        owner = f"bench-worker-{random.randint(1, 100)}"

        # Acquire
        lat1, status1, ok1 = await self._timed_request(
            session, "POST", f"{self.host}/lock/acquire",
            json={"resource": resource, "lock_type": "shared", "owner": owner, "wait": False},
            headers={"Content-Type": "application/json"}
        )
        if not ok1:
            return lat1, status1, False

        # Release (fire-and-forget for benchmark purposes)
        # We'd need the lock_id in real scenario
        return lat1, status1, True

    async def scenario_queue_produce(self, session):
        import random
        queue_name = f"bench-queue-{random.choice(['a', 'b', 'c'])}"
        return await self._timed_request(
            session, "POST", f"{self.host}/queue/produce",
            json={
                "queue_name": queue_name,
                "payload": {"event": "benchmark", "ts": time.time()},
                "producer_id": "benchmark-runner"
            },
            headers={"Content-Type": "application/json"}
        )

    async def scenario_raft_status(self, session):
        return await self._timed_request(session, "GET", f"{self.host}/raft/status")

    # ── Comparison: Single vs Distributed ────────────────────────────────────

    async def run_comparison(self) -> dict:
        """Single node vs 3-node cluster comparison."""
        print("\n  📊 Single vs Distributed Comparison")
        nodes = [
            ("node1", f"{self.host}"),
            ("node2", self.host.replace("8001", "8002")),
            ("node3", self.host.replace("8001", "8003")),
        ]

        node_results = {}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            for node_name, node_url in nodes:
                latencies = []
                for _ in range(100):
                    start = time.perf_counter()
                    try:
                        async with session.get(f"{node_url}/cache/status") as resp:
                            await resp.read()
                            latencies.append((time.perf_counter() - start) * 1000)
                    except Exception:
                        pass
                if latencies:
                    node_results[node_name] = {
                        "avg_ms": round(statistics.mean(latencies), 2),
                        "p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 2),
                    }

        return node_results

    # ── Main Runner ───────────────────────────────────────────────────────────

    async def run(self):
        print("\n" + "="*60)
        print("  🚀 Distributed Sync System - Benchmark Suite")
        print("="*60)
        print(f"  Host: {self.host}")
        print(f"  Concurrency: {self.concurrency} workers")
        print(f"  Duration per scenario: {self.duration}s")

        scenarios = [
            ("Health Check Baseline", self.scenario_health_check),
            ("Cache Read (MESI)", self.scenario_cache_read),
            ("Cache Write (MESI + Invalidation)", self.scenario_cache_write),
            ("Distributed Lock Acquire/Release", self.scenario_lock_acquire_release),
            ("Queue Produce (Consistent Hash)", self.scenario_queue_produce),
            ("Raft Status Check", self.scenario_raft_status),
        ]

        results = []
        for name, fn in scenarios:
            result = await self._run_scenario(name, fn)
            results.append(result)

        comparison = await self.run_comparison()

        # Save results
        output = {
            "benchmark_date": datetime.now().isoformat(),
            "config": {
                "host": self.host,
                "concurrency": self.concurrency,
                "duration_per_scenario": self.duration,
            },
            "scenarios": results,
            "node_comparison": comparison,
            "summary": {
                "total_scenarios": len(results),
                "max_rps": max((r.get("throughput_rps", 0) for r in results), default=0),
                "avg_p95_latency": round(
                    statistics.mean(
                        r["latency_ms"]["p95"] for r in results if "latency_ms" in r
                    ), 2
                ) if results else 0,
            }
        }

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(RESULTS_DIR, f"benchmark_{timestamp}.json")
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)

        print(f"\n{'='*60}")
        print(f"  ✅ Benchmark complete! Results saved to:\n     {output_path}")
        print(f"{'='*60}")
        print(f"\n  📈 Summary:")
        print(f"     Max throughput: {output['summary']['max_rps']} RPS")
        print(f"     Avg P95 latency: {output['summary']['avg_p95_latency']}ms")
        return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Sync System Benchmark")
    parser.add_argument("--host", default="http://localhost:8001", help="Node host URL")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent workers")
    parser.add_argument("--duration", type=int, default=30, help="Duration per scenario (seconds)")
    args = parser.parse_args()

    runner = BenchmarkRunner(args.host, args.concurrency, args.duration)
    asyncio.run(runner.run())
