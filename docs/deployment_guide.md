# Deployment Guide - Distributed Synchronization System

## Prerequisites

| Tool | Versi | Keperluan |
|---|---|---|
| Python | 3.8+ | Runtime aplikasi |
| Docker | 20+ | Containerization |
| Docker Compose | 2.0+ | Orchestration |
| Redis | 7.0+ | Persistent storage (bisa via Docker) |

---

## Cara 1: Local Development (tanpa Docker)

### Langkah 1 - Clone & Setup Environment

```bash
git clone https://github.com/[USERNAME]/distributed-sync-system.git
cd distributed-sync-system

# Buat virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### Langkah 2 - Konfigurasi Environment

```bash
cp .env.example .env
# Edit .env sesuai kebutuhan
```

### Langkah 3 - Jalankan Redis

```bash
# Dengan Docker:
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Atau install Redis langsung
```

### Langkah 4 - Jalankan 3 Nodes (3 terminal terpisah)

**Terminal 1 (Node 1):**
```bash
NODE_ID=node1 NODE_PORT=8001 \
PEERS=http://localhost:8002,http://localhost:8003 \
REDIS_HOST=localhost \
python -m uvicorn app:app --host 0.0.0.0 --port 8001
```

**Terminal 2 (Node 2):**
```bash
NODE_ID=node2 NODE_PORT=8002 \
PEERS=http://localhost:8001,http://localhost:8003 \
REDIS_HOST=localhost \
python -m uvicorn app:app --host 0.0.0.0 --port 8002
```

**Terminal 3 (Node 3):**
```bash
NODE_ID=node3 NODE_PORT=8003 \
PEERS=http://localhost:8001,http://localhost:8002 \
REDIS_HOST=localhost \
python -m uvicorn app:app --host 0.0.0.0 --port 8003
```

### Langkah 5 - Verifikasi

```bash
# Cek health semua nodes
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Cek Raft status (tunggu ~2-3 detik untuk leader election)
curl http://localhost:8001/raft/status
```

---

## Cara 2: Docker Compose (Recommended)

### Langkah 1 - Build Images

```bash
cd docker
docker compose build
```

### Langkah 2 - Jalankan Cluster

```bash
# Start semua services
docker compose up -d

# Lihat logs
docker compose logs -f

# Lihat logs node tertentu
docker compose logs -f node1
```

### Langkah 3 - Verifikasi Cluster

```bash
# Tunggu 10 detik untuk semua nodes ready
sleep 10

# Cek health
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Cek Raft leader election
curl http://localhost:8001/raft/status | python -m json.tool
```

### Langkah 4 - Akses Swagger UI

Buka browser dan akses:
- Node 1 API Docs: http://localhost:8001/docs
- Node 2 API Docs: http://localhost:8002/docs
- Node 3 API Docs: http://localhost:8003/docs

### Langkah 5 - Stop Cluster

```bash
docker compose down
# Dengan hapus volumes:
docker compose down -v
```

---

## Menjalankan Tests

### Unit Tests

```bash
# Semua tests
pytest tests/unit/ -v

# Test spesifik
pytest tests/unit/test_raft.py -v
pytest tests/unit/test_lock_manager.py -v
pytest tests/unit/test_cache.py -v
pytest tests/unit/test_queue.py -v
pytest tests/unit/test_security.py -v

# Dengan coverage report
pytest tests/unit/ -v --cov=src --cov-report=html
```

### Integration Tests

```bash
# Pastikan cluster berjalan dulu
pytest tests/integration/ -v
```

### Load Testing (Locust)

```bash
# Pastikan minimal 1 node berjalan

# Mode GUI (buka http://localhost:8089)
locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8001

# Mode Headless
locust -f benchmarks/load_test_scenarios.py \
    --host=http://localhost:8001 \
    --headless -u 50 -r 5 --run-time 60s \
    --csv=benchmarks/results/locust_result
```

### Automated Benchmark

```bash
python benchmarks/benchmark_runner.py \
    --host http://localhost:8001 \
    --concurrency 20 \
    --duration 30
```

---

## Demo Scenarios

### 1. Demo Lock Manager

```bash
# Acquire exclusive lock
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"resource": "database-shard-1", "lock_type": "exclusive", "owner": "process-A"}'

# Response: {"status": "granted", "lock_id": "node1:lock:1:...", ...}
LOCK_ID="[lock_id dari response]"

# Coba acquire lagi (harus DENIED karena exclusive)
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"resource": "database-shard-1", "lock_type": "exclusive", "owner": "process-B", "wait": false}'

# Release lock
curl -X POST http://localhost:8001/lock/release \
  -H "Content-Type: application/json" \
  -d "{\"lock_id\": \"$LOCK_ID\", \"owner\": \"process-A\"}"

# Demo deadlock scenario (otomatis)
curl http://localhost:8001/demo/scenario/lock-deadlock
```

### 2. Demo MESI Cache

```bash
# Write ke node1 (state: M)
curl -X POST http://localhost:8001/cache/write \
  -H "Content-Type: application/json" \
  -d '{"key": "user:123", "value": {"name": "Alice", "age": 25}}'

# Read dari node1 (cache HIT)
curl "http://localhost:8001/cache/read?key=user:123"

# Cek cache lines
curl http://localhost:8001/cache/lines

# Demo MESI (otomatis)
curl http://localhost:8001/demo/scenario/cache-mesi
```

### 3. Demo Distributed Queue

```bash
# Produce message
curl -X POST http://localhost:8001/queue/produce \
  -H "Content-Type: application/json" \
  -d '{"queue_name": "orders", "payload": {"product": "laptop", "qty": 1}, "producer_id": "shop"}'

# Consume message
curl "http://localhost:8001/queue/consume?queue_name=orders&consumer_id=warehouse&timeout=5"

# Demo queue flow (otomatis)
curl http://localhost:8001/demo/scenario/queue-flow
```

### 4. Demo Security (Bonus D)

```bash
# Login sebagai admin
curl -X POST http://localhost:8001/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin-user", "role": "admin"}'

# Simpan token
TOKEN="[token dari response]"

# Akses endpoint dengan token
curl -H "Authorization: Bearer $TOKEN" http://localhost:8001/auth/audit-log

# Dapatkan demo tokens untuk semua role
curl http://localhost:8001/auth/demo-tokens
```

### 5. Demo PBFT (Bonus A)

```bash
# Dapatkan token
TOKEN=$(curl -s -X POST http://localhost:8001/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "role": "admin"}' | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Submit PBFT request
curl -X POST http://localhost:8001/pbft/request \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"command": {"action": "SET_CONFIG", "key": "max_connections", "value": 100}}'

# Cek PBFT status
curl http://localhost:8001/pbft/status
```

---

## Troubleshooting

### Node tidak terdeteksi sebagai peer

**Gejala:** `GET /status` menampilkan semua peers sebagai `has_contact: false`

**Solusi:**
```bash
# Cek apakah semua nodes berjalan
docker compose ps

# Cek network connectivity
docker exec node1 curl http://node2:8002/health
docker exec node1 curl http://node3:8003/health

# Restart jika perlu
docker compose restart node1
```

### Raft tidak memilih leader

**Gejala:** Semua nodes tetap sebagai `follower`

**Penyebab & Solusi:**
- Nodes belum bisa saling berkomunikasi → Cek peers environment variable
- Network partition → Cek Docker network
- Tunggu lebih lama (~5 detik) untuk election timeout

```bash
# Paksa lihat logs election
docker compose logs node1 | grep -i "CANDIDATE\|LEADER\|election"
```

### Redis connection error

**Gejala:** `ConnectionError: Error connecting to Redis`

**Solusi:**
```bash
# Cek Redis status
docker compose ps redis
docker exec redis redis-cli ping  # Harus jawab PONG

# Cek REDIS_HOST env variable
echo $REDIS_HOST  # Harus "redis" di Docker, "localhost" di local
```

### Lock tidak di-release otomatis

Lock memiliki timeout 30 detik (configurable). Untuk force release:
```bash
curl http://localhost:8001/lock/status  # Lihat lock_id
curl -X POST http://localhost:8001/lock/release \
  -d '{"lock_id": "...", "owner": "..."}'
```

---

## Environment Variables Reference

| Variable | Default | Deskripsi |
|---|---|---|
| `NODE_ID` | `node1` | ID unik untuk node ini |
| `NODE_HOST` | `0.0.0.0` | Host untuk bind |
| `NODE_PORT` | `8001` | Port HTTP server |
| `PEERS` | `""` | Comma-separated peer URLs |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `RAFT_ELECTION_TIMEOUT_MIN` | `150` | Min election timeout (ms) |
| `RAFT_ELECTION_TIMEOUT_MAX` | `300` | Max election timeout (ms) |
| `RAFT_HEARTBEAT_INTERVAL` | `50` | Heartbeat interval (ms) |
| `CACHE_MAX_SIZE` | `1000` | Max cache entries per node |
| `SECRET_KEY` | *(change in prod!)* | JWT signing key |
| `LOG_LEVEL` | `INFO` | DEBUG/INFO/WARNING/ERROR |
| `METRICS_PORT` | `9090` | Prometheus metrics port |
