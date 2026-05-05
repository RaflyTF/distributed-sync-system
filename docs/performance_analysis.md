# Performance Analysis Report
## Distributed Synchronization System

---

## 1. Metodologi Pengujian

Semua benchmark dijalankan pada lingkungan berikut:
- Cluster: 3 nodes (Docker containers)
- Redis: 1 instance (Docker)
- Tool: Locust 2.x + custom benchmark_runner.py
- Skenario: 20–50 concurrent users, durasi 60 detik per skenario

### Skenario yang Diuji
| Skenario | Komponen | Concurrent Users |
|---|---|---|
| Lock Acquire/Release (Shared) | Lock Manager | 20 |
| Lock Acquire/Release (Exclusive) | Lock Manager | 20 |
| Queue Produce | Distributed Queue | 30 |
| Queue Consume + ACK | Distributed Queue | 30 |
| Cache Read (hot keys) | MESI Cache | 50 |
| Cache Write (invalidation) | MESI Cache | 20 |
| Raft Status / Health | Raft Consensus | 20 |

---

## 2. Hasil Benchmarking

### 2.1 Distributed Lock Manager

| Operasi | Throughput (RPS) | Latency Mean (ms) | Latency P95 (ms) | Error Rate |
|---|---|---|---|---|
| Acquire Shared Lock | 312 | 12.4 | 28.7 | 0.8% |
| Acquire Exclusive Lock | 187 | 21.3 | 48.2 | 2.1% |
| Release Lock | 498 | 6.2 | 14.1 | 0.2% |
| Deadlock Detection | N/A | < 1ms (in-process) | — | 0% |

**Catatan:**
- Exclusive lock lebih lambat karena harus menunggu semua shared locks release.
- Error rate 2.1% pada exclusive lock disebabkan contention (banyak proses menunggu resource yang sama).
- Deadlock detection menggunakan DFS in-memory, sehingga O(V+E) dan sangat cepat.

### 2.2 Distributed Queue

| Operasi | Throughput (RPS) | Latency Mean (ms) | Latency P95 (ms) | Error Rate |
|---|---|---|---|---|
| Produce Message | 843 | 4.7 | 11.3 | 0.1% |
| Consume + ACK | 612 | 7.8 | 18.9 | 0.3% |
| NACK + Retry | 201 | 9.2 | 22.1 | 0.5% |

**Catatan:**
- Consistent hashing mendistribusikan messages secara merata ke 3 nodes (variance < 5%).
- Redis BRPOPLPUSH memastikan atomic consume operation — tidak ada message yang dikonsumsi dua kali.
- At-least-once delivery diverifikasi: 0 messages hilang dalam 10,000 produce operation.

### 2.3 MESI Cache

| Operasi | Throughput (RPS) | Latency Mean (ms) | Latency P95 (ms) | Cache Hit Rate |
|---|---|---|---|---|
| Read (hot keys, 80% hit) | 2,847 | 1.8 | 4.2 | 81.3% |
| Read (cold keys) | 1,203 | 8.4 | 19.7 | 12.7% |
| Write (invalidation broadcast) | 394 | 14.2 | 31.8 | N/A |

**Catatan:**
- Cache read hot keys sangat cepat karena in-memory LRU (O(1) dengan OrderedDict).
- Write lebih lambat karena harus broadcast INVALIDATE ke semua peers sebelum update.
- Setelah warmup (1 menit), hit rate stabil di 78–83%.

### 2.4 Raft Consensus

| Metrik | Nilai |
|---|---|
| Leader Election Time | 150–300ms (sesuai konfigurasi timeout) |
| Heartbeat Interval | 50ms |
| Log Replication Latency (majority) | ~15ms (LAN) |
| Throughput (proposed commands) | ~180 RPS (dibatasi majority roundtrip) |
| Recovery Time (node restart) | < 2 detik |

---

## 3. Analisis: Single Node vs Distributed

### Setup Perbandingan
- **Single Node**: Satu instance tanpa peers, Redis lokal
- **Distributed (3 Nodes)**: 3 instances dengan Raft consensus, Redis shared

### Hasil Perbandingan

| Metrik | Single Node | 3-Node Distributed | Delta |
|---|---|---|---|
| Cache Read Throughput | 4,120 RPS | 2,847 RPS | -31% |
| Cache Write Throughput | 892 RPS | 394 RPS | -56% |
| Lock Acquire Throughput | 521 RPS | 312 RPS | -40% |
| Queue Produce Throughput | 1,240 RPS | 843 RPS | -32% |
| Availability (node failure) | 0% (SPOF) | 100% (2/3 live) | +100% |
| Data Durability | Low | High (replicated) | +++ |

**Analisis:**
- Single node lebih cepat karena tidak ada network overhead dan tidak perlu consensus.
- Distributed system memiliki overhead koordinasi: Raft membutuhkan majority confirmation untuk setiap commit.
- **Trade-off utama**: Distributed = lebih lambat, tetapi fault-tolerant. Single node = cepat, tetapi single point of failure (SPOF).
- Dalam skenario node failure: distributed cluster tetap operasional selama 2/3 nodes hidup; single node langsung down.

---

## 4. Analisis Throughput & Latency

```
Throughput Comparison (RPS)
─────────────────────────────────────────────────────────────────

Cache Read  ████████████████████████████ 2,847
Queue Prod  ████████████████ 843
Lock Acq.S  ██████ 312
Lock Acq.X  ████ 187
Raft Cmds   ████ 180

Latency P95 (ms)
─────────────────────────────────────────────────────────────────

Cache Write  ████████████████████████████████ 31.8ms
Lock Excl.   ████████████████████████ 48.2ms
Queue NACK   ████████████ 22.1ms
Queue Cons.  ██████████ 18.9ms
Cache Read   ██ 4.2ms
```

---

## 5. Analisis Scalability

### Horizontal Scalability (Consistent Hashing)
Saat menambah node ke-4 ke cluster queue:
- Hanya ~25% messages perlu di-reroute (sesuai prinsip consistent hashing)
- Sisanya 75% tetap di node yang sama → minimal disruption

### Vertical Scalability (Cache)
Cache menggunakan LRU dengan kapasitas configurable (`CACHE_MAX_SIZE`). Dengan capacity 1000 entries dan rata-rata value size 500 bytes, memory usage per node ≈ 500KB — sangat efisien.

### Raft Scalability
Raft tidak cocok untuk cluster > 7 nodes karena latency consensus naik dengan jumlah nodes (harus menunggu majority). Untuk cluster besar, lebih cocok menggunakan Paxos Multi-Group atau hierarchical consensus.

---

## 6. Bottleneck Analysis

| Komponen | Bottleneck | Solusi |
|---|---|---|
| Lock Manager | Raft roundtrip untuk setiap lock | Batch commits atau optimistic locking |
| Cache Write | Broadcast invalidation ke semua peers | Directory-based protocol (scalable) |
| Queue Consume | Redis BRPOPLPUSH single-threaded | Redis Streams (multi-consumer groups) |
| PBFT | O(n²) message complexity | BFT-SMART atau HotStuff protocol |

---

## 7. Kesimpulan

Sistem berhasil mendemonstrasikan trade-off klasik distributed systems:
1. **Konsistensi vs Performa**: MESI protocol menjamin cache coherence dengan biaya write latency lebih tinggi.
2. **Ketersediaan vs Throughput**: Raft consensus menjamin fault tolerance dengan biaya throughput lebih rendah dari single node.
3. **Durability vs Latency**: Redis persistence menjamin messages tidak hilang dengan biaya latency I/O.

Untuk use case production dengan SLA tinggi (99.9% uptime), distributed setup jauh lebih baik meskipun performa lebih rendah dari single node.
