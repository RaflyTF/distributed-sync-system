# Distributed Synchronization System

> **Tugas 2 - Sistem Parallel dan Terdistribusi**
> Implementasi Distributed Synchronization System dengan Raft Consensus, MESI Cache, Distributed Queue, PBFT, dan Security Layer.

---

## Fitur yang Diimplementasikan

### Core (70 poin)
| Komponen | Algoritma | Status |
|---|---|---|
| Distributed Lock Manager | Raft Consensus + RAG Deadlock Detection | ✅ |
| Distributed Queue | Consistent Hashing + At-least-once delivery | ✅ |
| MESI Cache Coherence | MESI Protocol + LRU Replacement | ✅ |
| Containerization | Docker + Docker Compose | ✅ |

### Bonus
| Fitur | Algoritma | Poin |
|---|---|---|
| PBFT (Bonus A) | Practical Byzantine Fault Tolerance | +5 |
| Security (Bonus D) | JWT + RBAC + Tamper-proof Audit Log | +5 |

**Total Potensi: 70 + 20 (docs) + 10 (video) + 10 (bonus) = 110 poin**

---

## Quick Start

### Dengan Docker Compose (Rekomendasi)

```bash
# Clone repository
git clone https://github.com/[USERNAME]/distributed-sync-system
cd distributed-sync-system

# Start 3-node cluster + Redis
cd docker && docker compose up -d

# Tunggu 10 detik, lalu cek status
curl http://localhost:8001/raft/status
```

### Local Development

```bash
pip install -r requirements.txt
cp .env.example .env

# Terminal 1 - Node 1
NODE_ID=node1 NODE_PORT=8001 PEERS=http://localhost:8002,http://localhost:8003 \
REDIS_HOST=localhost python -m uvicorn app:app --port 8001

# Terminal 2 - Node 2
NODE_ID=node2 NODE_PORT=8002 PEERS=http://localhost:8001,http://localhost:8003 \
REDIS_HOST=localhost python -m uvicorn app:app --port 8002

# Terminal 3 - Node 3
NODE_ID=node3 NODE_PORT=8003 PEERS=http://localhost:8001,http://localhost:8002 \
REDIS_HOST=localhost python -m uvicorn app:app --port 8003
```

---

## Arsitektur

```
┌─────────────────────────────────────────────────────────────┐
│                      CLIENT / REST API                       │
└──────────────────────┬──────────────────────────────────────┘
                       │
      ┌────────────────┼────────────────┐
      │                │                │
 ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
 │  NODE 1 │◄────►│  NODE 2 │◄────►│  NODE 3 │
 │  :8001  │      │  :8002  │      │  :8003  │
 └────┬────┘      └────┬────┘      └────┬────┘
      │                │                │
      └────────────────┼────────────────┘
                       ▼
                 ┌──────────┐
                 │  Redis   │
                 │  :6379   │
                 └──────────┘
```

**Setiap Node berisi:**
- Raft Consensus Engine (leader election & log replication)
- Distributed Lock Manager (shared/exclusive locks + deadlock detection)
- MESI Cache Node (cache coherence protocol)
- Distributed Queue (consistent hashing + Redis persistence)
- PBFT Engine (Byzantine fault tolerance)
- Security Layer (JWT auth + RBAC + audit log)
- Phi Accrual Failure Detector

---

## Menjalankan Tests

```bash
# Unit tests semua komponen
pytest tests/unit/ -v

# Test dengan coverage
pytest tests/unit/ -v --cov=src --cov-report=html
open htmlcov/index.html
```

---

## Load Testing

```bash
# Locust GUI (http://localhost:8089)
locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8001

# Automated benchmark
python benchmarks/benchmark_runner.py --host http://localhost:8001 --duration 30
```

---

## API Documentation

- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc
- OpenAPI Spec: `docs/api_spec.yaml`

### Contoh API Calls

```bash
# Dapatkan demo tokens
curl http://localhost:8001/auth/demo-tokens

# Acquire lock
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"resource": "shared-db", "lock_type": "exclusive", "owner": "worker-1"}'

# Write ke cache
curl -X POST http://localhost:8001/cache/write \
  -H "Content-Type: application/json" \
  -d '{"key": "user:42", "value": {"name": "Alice"}}'

# Produce ke queue
curl -X POST http://localhost:8001/queue/produce \
  -H "Content-Type: application/json" \
  -d '{"queue_name": "events", "payload": {"type": "order_created"}}'

# Demo scenarios
curl http://localhost:8001/demo/scenario/lock-deadlock
curl http://localhost:8001/demo/scenario/cache-mesi
curl http://localhost:8001/demo/scenario/queue-flow
```

---

## Struktur Proyek

```
distributed-sync-system/
├── src/
│   ├── nodes/
│   │   ├── base_node.py       # Security Layer (JWT, RBAC, Audit)
│   │   ├── lock_manager.py    # Distributed Lock + Deadlock Detection
│   │   ├── queue_node.py      # Distributed Queue + Consistent Hashing
│   │   └── cache_node.py      # MESI Cache Coherence + LRU
│   ├── consensus/
│   │   ├── raft.py            # Raft Consensus Algorithm
│   │   └── pbft.py            # PBFT Byzantine Fault Tolerance
│   ├── communication/
│   │   ├── message_passing.py # Async HTTP + Circuit Breaker + JWT
│   │   └── failure_detector.py# Phi Accrual Failure Detector
│   └── utils/
│       ├── config.py           # Environment configuration
│       └── metrics.py          # Prometheus metrics
├── tests/
│   ├── unit/                   # Unit tests (pytest)
│   └── integration/            # Integration tests
├── docker/
│   ├── Dockerfile.node         # Node container image
│   ├── docker-compose.yml      # 3-node cluster orchestration
│   └── prometheus.yml          # Metrics scraping config
├── benchmarks/
│   ├── load_test_scenarios.py  # Locust load test scenarios
│   └── benchmark_runner.py     # Automated benchmark runner
├── docs/
│   ├── architecture.md         # System architecture documentation
│   ├── api_spec.yaml           # OpenAPI 3.0 specification
│   └── deployment_guide.md     # Step-by-step deployment guide
├── app.py                      # FastAPI main application
├── requirements.txt
├── .env.example
└── README.md
```

---

## Video Demo

🎬 **Link YouTube**: https://youtu.be/rh8xOmxW0WQ?si=VK3_jM_O4n6HIis2

Struktur video (10-15 menit):
1. Pendahuluan dan tujuan (1-2 menit)
2. Penjelasan arsitektur sistem (2-3 menit)
3. Live demo semua fitur (5-7 menit)
4. Performance testing (2-3 menit)
5. Kesimpulan dan tantangan (1-2 menit)

---

## Teknologi yang Digunakan

| Kategori | Library/Tool |
|---|---|
| Runtime | Python 3.11, asyncio |
| Web Framework | FastAPI, uvicorn |
| Storage | Redis (persistence + queue) |
| Networking | aiohttp (async HTTP) |
| Security | PyJWT, cryptography |
| Containerization | Docker, Docker Compose |
| Monitoring | Prometheus client, Grafana |
| Testing | pytest, pytest-asyncio |
| Load Testing | Locust |

---

## Algoritma & Referensi

1. **Raft**: Ongaro, D., & Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm*. USENIX ATC.
2. **PBFT**: Castro, M., & Liskov, B. (1999). *Practical Byzantine Fault Tolerance*. OSDI.
3. **MESI**: Papamarcos, M., & Patel, J. (1984). *A Low-Overhead Coherence Solution for Multiprocessors with Private Cache Memories*. ISCA.
4. **Consistent Hashing**: Karger, D. et al. (1997). *Consistent Hashing and Random Trees*. STOC.
5. **Phi Accrual**: Hayashibara, N. et al. (2004). *The Φ Accrual Failure Detector*. IEEE SRDS.

---

## Identitas

- **Nama**: Rafly Taufika Fikri
- **NIM**: 11231083
- **Mata Kuliah**: Sistem Parallel dan Terdistribusi
- **Tahun**: 2026
