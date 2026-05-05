# Arsitektur Sistem - Distributed Synchronization System

## Gambaran Umum

Sistem ini mengimplementasikan distributed synchronization framework yang mampu menangani skenario real-world pada lingkungan terdistribusi. Setiap node menjalankan empat komponen utama secara bersamaan: Distributed Lock Manager, Distributed Queue, MESI Cache, dan Raft Consensus.

```
┌─────────────────────────────────────────────────────────────────┐
│                     CLIENT / API CONSUMERS                       │
└────────────────────────┬────────────────────────────────────────┘
                         │ HTTP/REST
         ┌───────────────┼───────────────────┐
         │               │                   │
    ┌────▼────┐     ┌────▼────┐        ┌────▼────┐
    │  NODE 1 │     │  NODE 2 │        │  NODE 3 │
    │ :8001   │◄───►│ :8002   │◄──────►│ :8003   │
    └────┬────┘     └────┬────┘        └────┬────┘
         │               │                   │
         └───────────────┼───────────────────┘
                         │
                    ┌────▼────┐
                    │  Redis  │
                    │  :6379  │
                    └─────────┘
```

## Komponen Sistem

### 1. Raft Consensus (`src/consensus/raft.py`)

Raft digunakan sebagai backbone untuk replikasi keputusan lock (Distributed Lock Manager).

**Implementasi:**
- **Leader Election**: Randomized election timeout (150–300ms). Kandidat mengumpulkan majority votes sebelum menjadi leader.
- **Log Replication**: Leader mereplikasi setiap log entry ke semua follower menggunakan AppendEntries RPC. Entry dianggap committed jika majority nodes mengkonfirmasi.
- **Safety**: Hanya node dengan log paling up-to-date yang bisa menjadi leader (log completeness property).
- **Heartbeat**: Leader mengirim heartbeat setiap 50ms untuk mencegah election timeout di follower.

**State Machine Transitions:**
```
FOLLOWER ──(election timeout)──► CANDIDATE ──(majority votes)──► LEADER
    ▲                                │                              │
    └──────────(higher term)─────────┘◄─────(higher term)──────────┘
```

**RPC Calls:**
| Endpoint | Direction | Tujuan |
|---|---|---|
| `POST /raft/request-vote` | Candidate → All peers | Meminta suara untuk election |
| `POST /raft/append-entries` | Leader → All followers | Replikasi log / heartbeat |

---

### 2. PBFT - Practical Byzantine Fault Tolerance (`src/consensus/pbft.py`) *(Bonus A)*

PBFT digunakan untuk skenario yang membutuhkan ketahanan terhadap Byzantine failures (node yang mengirim data salah/malicious).

**Formula**: Toleransi f faulty nodes dengan n ≥ 3f + 1 total nodes.

**3 Fase Protokol:**
```
CLIENT                PRIMARY              REPLICA 1          REPLICA 2
  │                      │                     │                   │
  │──── REQUEST ─────────►│                     │                   │
  │                      │──── PRE-PREPARE ────►│──── PRE-PREPARE ──►│
  │                      │                     │──── PREPARE ──────►│
  │                      │◄────────────────────│◄──── PREPARE ──────│
  │                      │──── COMMIT ─────────►│──── COMMIT ───────►│
  │◄──── REPLY ──────────│◄────────────────────│                   │
```

**Perbedaan dengan Raft:**
| Aspek | Raft | PBFT |
|---|---|---|
| Fault Model | Crash-fault tolerant | Byzantine-fault tolerant |
| Node Minimum | 3 nodes | 4 nodes (3f+1) |
| Phases | 2 (election + replicate) | 3 (pre-prepare, prepare, commit) |
| Throughput | Lebih tinggi | Lebih rendah (message complexity O(n²)) |

---

### 3. Distributed Lock Manager (`src/nodes/lock_manager.py`)

**Lock Types:**
- **Shared Lock (S)**: Multiple readers diperbolehkan secara bersamaan.
- **Exclusive Lock (X)**: Hanya satu writer; tidak kompatibel dengan lock lain.

**Compatibility Matrix:**
```
           | Shared (S) | Exclusive (X)
-----------+------------+--------------
Shared (S) |    ✓ OK    |    ✗ Conflict
Exclusive  |  ✗ Conflict|    ✗ Conflict
```

**Deadlock Detection - Resource Allocation Graph (RAG):**

RAG adalah directed graph dimana:
- Node = proses (owners) atau resources
- **Assignment edge** (resource → process): resource sedang dipakai process
- **Request edge** (process → resource): process menunggu resource

Deadlock terdeteksi jika ada **siklus** dalam RAG.

```
   Deadlock Scenario:
   P1 holds R1, waits for R2
   P2 holds R2, waits for R1
   
   P1 ──waits──► R2 ──held by──► P2 ──waits──► R1 ──held by──► P1 (CYCLE!)
```

**Algoritma Deteksi**: DFS dengan path tracking. Time complexity: O(V + E).

**Lock Lifecycle:**
```
REQUESTED → [can_grant?] → GRANTED → [hold duration] → RELEASED
                │                                           ▲
                ▼ (no)                                      │
            WAITING ──(timeout?)──► DENIED                  │
                │                                           │
                ▼ (deadlock?)                               │
            DEADLOCK ──(victim selection)──► force RELEASE ─┘
```

---

### 4. Distributed Queue (`src/nodes/queue_node.py`)

**Consistent Hashing Ring:**

Setiap node memiliki 150 virtual nodes (vnodes) di ring untuk distribusi merata.

```
           Node1-vnode-0
              (hash: 1234)
      ┌─────────────────────────┐
      │         Ring            │
  Node3 ─────────►●─────────── Node1
  vnode-42   (clockwise)   vnode-17
      │                        │
  Node2-vnode-99 ────────── Node3-vnode-7
      └─────────────────────────┘
```

Untuk key "queue-orders": hash → cari node pertama searah jarum jam.

**Delivery Guarantee (At-Least-Once):**
```
PRODUCE                    CONSUME                     ACK/NACK
   │                          │                           │
   ▼                          ▼                           ▼
Redis LPUSH              BRPOPLPUSH                 LREM + DEL
(pending list)     (pending → processing)       (remove from processing)
                                                          │
                                               [nack] LPUSH back to pending
                                               [max retries] → Dead Letter Queue
```

**Node Failure Recovery:**
- Messages tersimpan di Redis (persistent)
- Hash ring di-update saat node failure
- Processing queue di-recover saat consumer restart (`/queue/recover`)

---

### 5. MESI Cache Coherence (`src/nodes/cache_node.py`)

**State Diagram:**
```
         ┌─────────────────────────────────────────┐
         │                                         │
         ▼                                         │
    ┌─────────┐  write (no others)  ┌──────────┐   │
    │ INVALID │ ──────────────────► │ MODIFIED │   │
    │   (I)   │                     │   (M)    │   │
    └─────────┘  read (no others)   └──────────┘   │
         │      ──────────────────► ┌──────────┐   │ other writes
         │                          │EXCLUSIVE │ ──┘
         │      read (others exist) │   (E)    │
         │      ──────────────────► └──────────┘
         │                               │ other reads
         │                          ┌────▼─────┐
         └─────────────────────────►│  SHARED  │
                                    │   (S)    │
                                    └──────────┘
```

**Protocol Operations:**
| Operasi | Prosedur |
|---|---|
| Read (I state) | Broadcast ReadShared → peers respond. E state jika exclusive, S jika shared. |
| Write (any state) | Broadcast Invalidate → all peers go to I → sender goes to M. |
| M state: peer reads | Flush (writeback to memory) → sender goes to S, new reader gets S. |
| E state: peer reads | Both go to S (no writeback needed, data already clean). |

**LRU Replacement Policy:**
Menggunakan `OrderedDict` Python untuk O(1) get/put. Item paling lama tidak diakses dieviksi saat kapasitas penuh. Jika evicted item dalam state M, dilakukan writeback ke memory dulu.

---

### 6. Security Layer - Bonus D (`src/nodes/base_node.py`)

**JWT Authentication:**
```
Client ──[POST /auth/login]──► Server ──[JWT Token]──► Client
Client ──[GET /lock/acquire + Bearer Token]──► Server
                                                   │
                                              Verify JWT
                                                   │
                                              Check RBAC
                                                   │
                                           Allow / Deny
```

**RBAC Hierarchy:**
```
admin      → Semua permissions
producer   → queue:produce, cache:read
consumer   → queue:consume, lock:acquire, lock:release, cache:read
reader     → Read-only access
node       → Inter-node communication permissions
```

**Audit Log Chain:**
```
Entry[0]: {data, prev_hash: "genesis", hash: H0, sig: S0}
Entry[1]: {data, prev_hash: H0,        hash: H1, sig: S1}
Entry[2]: {data, prev_hash: H1,        hash: H2, sig: S2}
```
Setiap entry di-hash bersama hash entry sebelumnya, membuat blockchain-like structure. Tamper detection = recompute chain.

---

### 7. Failure Detector (`src/communication/failure_detector.py`)

Menggunakan **Phi Accrual Algorithm** (dipakai Cassandra, Akka):
- Lebih akurat dari simple timeout
- Adaptif terhadap network conditions
- Output: nilai phi (suspicion level), bukan binary alive/dead

```
phi < 1   → Node almost certainly alive
phi 4-6   → Node might be having issues
phi > 8   → Node almost certainly dead
```

---

## Deployment Architecture

### Docker Compose (3 nodes + Redis):
```
┌─ docker network: distributed_net (172.20.0.0/16) ──────────────────┐
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ node1:8001   │  │ node2:8002   │  │ node3:8003   │              │
│  │ 172.20.0.11  │  │ 172.20.0.12  │  │ 172.20.0.13  │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                  │                  │                      │
│         └──────────────────┼──────────────────┘                     │
│                            │                                         │
│                   ┌────────▼────────┐                               │
│                   │  redis:6379     │                               │
│                   │  172.20.0.10    │                               │
│                   └─────────────────┘                               │
└──────────────────────────────────────────────────────────────────────┘
```

### Scaling:
Untuk menambah node (contoh node4):
1. Update `.env` dengan `NODE_ID=node4, NODE_PORT=8004, PEERS=http://node1:8001,...`
2. `docker-compose up -d node4`
3. Hash ring otomatis menyesuaikan distribusi queue
