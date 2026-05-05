"""
Main Application - Distributed Synchronization System
Menggabungkan semua komponen dalam satu FastAPI server per node.

Setiap node menjalankan:
- Raft Consensus (untuk lock management)
- PBFT (Bonus A: Byzantine fault tolerance)
- Distributed Lock Manager
- Distributed Queue
- MESI Cache
- Security Layer (Bonus D: JWT + RBAC + Audit)
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Body, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.utils.config import config
from src.utils.metrics import start_metrics_server
from src.consensus.raft import RaftNode
from src.consensus.pbft import PBFTNode
from src.nodes.lock_manager import DistributedLockManager, LockType
from src.nodes.queue_node import DistributedQueue
from src.nodes.cache_node import MESICacheNode
from src.nodes.base_node import AuthToken, AuditLogger, RequestValidator, create_demo_tokens, ROLES
from src.communication.failure_detector import FailureDetector
from src.utils.cert_manager import CertificateManager

# ── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Global Components (initialized on startup) ────────────────────────────────
raft: Optional[RaftNode] = None
pbft: Optional[PBFTNode] = None
lock_mgr: Optional[DistributedLockManager] = None
queue: Optional[DistributedQueue] = None
cache: Optional[MESICacheNode] = None
failure_detector: Optional[FailureDetector] = None
audit: Optional[AuditLogger] = None
validator: Optional[RequestValidator] = None
cert_mgr: Optional[CertificateManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle manager."""
    global raft, pbft, lock_mgr, queue, cache, failure_detector, audit, validator, cert_mgr

    logger.info(f"🚀 Starting node {config.NODE_ID} on port {config.NODE_PORT}")
    logger.info(f"   Peers: {config.PEERS}")

    # Initialize security layer (Bonus D)
    audit = AuditLogger(config.NODE_ID)
    validator = RequestValidator(audit)

    # Initialize Certificate Manager (Bonus D: Certificate management)
    cert_mgr = CertificateManager(config.NODE_ID)
    try:
        cert_mgr.load_or_generate()
        logger.info(f"[App] Certificate ready for node {config.NODE_ID}")
    except Exception as e:
        logger.warning(f"[App] Certificate generation failed (cryptography lib): {e}")

    # Initialize Raft
    raft = RaftNode(config.NODE_ID, config.PEERS)
    await raft.start()

    # Initialize PBFT (Bonus A)
    pbft = PBFTNode(config.NODE_ID, config.PEERS)

    # Initialize Lock Manager (uses Raft)
    lock_mgr = DistributedLockManager(raft)
    await lock_mgr.start()

    # Initialize Queue
    node_url = f"http://{config.NODE_HOST}:{config.NODE_PORT}"
    queue = DistributedQueue(config.NODE_ID, node_url, config.PEERS)
    await queue.start()

    # Initialize MESI Cache
    cache = MESICacheNode(config.NODE_ID, config.PEERS)
    await cache.start()

    # Initialize Failure Detector
    if config.PEERS:
        failure_detector = FailureDetector(config.NODE_ID, config.PEERS)

        async def on_peer_failure(peer: str):
            logger.warning(f"[App] Peer failure detected: {peer}")
            await queue.handle_node_failure(peer)
            audit.log("NODE_FAILURE", "system", peer, "failure_detected", "ALERT")

        failure_detector.on_peer_failure(on_peer_failure)
        await failure_detector.start()

    # Start Prometheus metrics
    start_metrics_server()

    logger.info(f"✅ Node {config.NODE_ID} ready!")
    audit.log("SYSTEM", "system", "node", "startup", "SUCCESS",
              {"peers": config.PEERS, "port": config.NODE_PORT})

    yield

    # Shutdown
    logger.info(f"⏹️  Shutting down node {config.NODE_ID}...")
    await raft.stop()
    await lock_mgr.stop()
    await queue.stop()
    await cache.stop()
    if failure_detector:
        await failure_detector.stop()


# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(
    title=f"Distributed Sync System - Node {config.NODE_ID}",
    description="Implementasi Distributed Synchronization System dengan Raft, MESI Cache, Distributed Queue",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Auth Helper ───────────────────────────────────────────────────────────────
def get_auth_payload(authorization: Optional[str]) -> dict:
    """Extract auth payload. Returns None payload for unauthenticated."""
    if not authorization:
        return {"sub": "anonymous", "role": "reader", "permissions": ROLES["reader"]["permissions"]}
    payload = validator.authenticate(authorization)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return payload


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH & STATUS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "node_id": config.NODE_ID, "timestamp": __import__("time").time()}


@app.get("/status", tags=["System"])
async def status():
    return {
        "node_id": config.NODE_ID,
        "raft": raft.get_status() if raft else None,
        "pbft": pbft.get_status() if pbft else None,
        "locks": lock_mgr.get_status() if lock_mgr else None,
        "cache": cache.get_status() if cache else None,
        "queue_ring": queue.get_ring_info() if queue else None,
        "peers": {
            peer: info
            for peer, info in (failure_detector.get_peer_status().items() if failure_detector else {}).items()
        },
        "audit": audit.get_stats() if audit else None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# RAFT RPC ENDPOINTS (internal, node-to-node)
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/raft/request-vote", tags=["Raft"], include_in_schema=False)
async def raft_request_vote(body: dict = Body(...)):
    result = await raft.handle_request_vote(body)
    return result


@app.post("/raft/append-entries", tags=["Raft"], include_in_schema=False)
async def raft_append_entries(body: dict = Body(...)):
    result = await raft.handle_append_entries(body)
    return result


@app.get("/raft/status", tags=["Raft"])
async def raft_status():
    return raft.get_status()


# ══════════════════════════════════════════════════════════════════════════════
# PBFT ENDPOINTS (Bonus A)
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/pbft/pre-prepare", tags=["PBFT (Bonus)"], include_in_schema=False)
async def pbft_pre_prepare(body: dict = Body(...)):
    return await pbft.handle_pre_prepare(body)


@app.post("/pbft/prepare", tags=["PBFT (Bonus)"], include_in_schema=False)
async def pbft_prepare(body: dict = Body(...)):
    return await pbft.handle_prepare(body)


@app.post("/pbft/commit", tags=["PBFT (Bonus)"], include_in_schema=False)
async def pbft_commit(body: dict = Body(...)):
    return await pbft.handle_commit(body)


class PBFTRequestModel(BaseModel):
    command: dict


@app.post("/pbft/request", tags=["PBFT (Bonus)"])
async def pbft_request(
    body: PBFTRequestModel,
    authorization: Optional[str] = Header(default=None)
):
    """Submit a PBFT consensus request (Byzantine fault tolerant)."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "pbft:request", "pbft"):
        raise HTTPException(status_code=403, detail="Permission denied")

    result = await pbft.request(body.command)
    return {"result": result, "pbft_status": pbft.get_status()}


@app.get("/pbft/status", tags=["PBFT (Bonus)"])
async def pbft_status():
    return pbft.get_status()


# ══════════════════════════════════════════════════════════════════════════════
# LOCK MANAGER ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

class AcquireLockRequest(BaseModel):
    resource: str
    lock_type: str = "exclusive"  # "shared" or "exclusive"
    owner: str
    timeout: float = 30.0
    wait: bool = True


@app.post("/lock/acquire", tags=["Lock Manager"])
async def acquire_lock(
    body: AcquireLockRequest,
    authorization: Optional[str] = Header(default=None)
):
    """
    Acquire a distributed lock on a resource.
    - lock_type: 'shared' (multiple readers) or 'exclusive' (single writer)
    - wait: whether to block until lock is available
    """
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "lock:acquire", body.resource):
        raise HTTPException(status_code=403, detail="Permission denied")

    try:
        lock_type = LockType(body.lock_type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid lock_type: {body.lock_type}")

    result = await lock_mgr.acquire(
        resource=body.resource,
        lock_type=lock_type,
        owner=body.owner,
        timeout=body.timeout,
        wait=body.wait,
    )
    audit.log("LOCK", payload["sub"], body.resource, f"acquire:{body.lock_type}", result["status"])
    return result


class ReleaseLockRequest(BaseModel):
    lock_id: str
    owner: str


@app.post("/lock/release", tags=["Lock Manager"])
async def release_lock(
    body: ReleaseLockRequest,
    authorization: Optional[str] = Header(default=None)
):
    """Release a previously acquired lock."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "lock:release", body.lock_id):
        raise HTTPException(status_code=403, detail="Permission denied")

    result = await lock_mgr.release(body.lock_id, body.owner)
    audit.log("LOCK", payload["sub"], body.lock_id, "release", result["status"])
    return result


@app.get("/lock/status", tags=["Lock Manager"])
async def lock_status():
    """Get current lock state and Resource Allocation Graph."""
    return lock_mgr.get_status()


# ══════════════════════════════════════════════════════════════════════════════
# QUEUE ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

class ProduceRequest(BaseModel):
    queue_name: str
    payload: dict
    producer_id: str = "default"


@app.post("/queue/produce", tags=["Queue"])
async def produce_message(
    body: ProduceRequest,
    authorization: Optional[str] = Header(default=None)
):
    """Produce a message to a distributed queue."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "queue:produce", body.queue_name):
        raise HTTPException(status_code=403, detail="Permission denied")

    msg_id = await queue.produce(body.queue_name, body.payload, body.producer_id)
    audit.log("QUEUE", payload["sub"], body.queue_name, "produce", "SUCCESS", {"msg_id": msg_id})
    return {"message_id": msg_id, "queue_name": body.queue_name}


@app.get("/queue/consume", tags=["Queue"])
async def consume_message(
    queue_name: str = Query(...),
    consumer_id: str = Query(default="consumer1"),
    timeout: float = Query(default=2.0),
    authorization: Optional[str] = Header(default=None)
):
    """Consume a message from a queue (blocking with timeout)."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "queue:consume", queue_name):
        raise HTTPException(status_code=403, detail="Permission denied")

    msg = await queue.consume(queue_name, consumer_id, timeout)
    if msg:
        audit.log("QUEUE", payload["sub"], queue_name, "consume", "SUCCESS", {"msg_id": msg.get("message_id")})
    return {"message": msg, "queue_name": queue_name}


class AckRequest(BaseModel):
    queue_name: str
    message_id: str
    consumer_id: str


@app.post("/queue/ack", tags=["Queue"])
async def acknowledge_message(body: AckRequest, authorization: Optional[str] = Header(default=None)):
    """Acknowledge successful message processing."""
    payload = get_auth_payload(authorization)
    result = await queue.acknowledge(body.queue_name, body.message_id, body.consumer_id)
    return {"acknowledged": result, "message_id": body.message_id}


@app.post("/queue/nack", tags=["Queue"])
async def nack_message(body: AckRequest, authorization: Optional[str] = Header(default=None)):
    """Negative acknowledge: re-queue or move to DLQ."""
    payload = get_auth_payload(authorization)
    result = await queue.nack(body.queue_name, body.message_id, body.consumer_id)
    return {"nacked": result, "message_id": body.message_id}


@app.post("/queue/replicate", tags=["Queue"], include_in_schema=False)
async def replicate_message(body: dict = Body(...)):
    """Internal: replicate a message to backup node."""
    from src.nodes.queue_node import Message
    msg = Message.from_dict(body["message"])
    await queue._persist_message(msg)
    return {"replicated": True}


@app.get("/queue/stats", tags=["Queue"])
async def queue_stats(queue_name: str = Query(...)):
    """Get queue statistics."""
    return await queue.get_queue_stats(queue_name)


# ══════════════════════════════════════════════════════════════════════════════
# CACHE ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/cache/read", tags=["Cache (MESI)"])
async def cache_read(
    key: str = Query(...),
    authorization: Optional[str] = Header(default=None)
):
    """Read a value from the distributed MESI cache."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "cache:read", key):
        raise HTTPException(status_code=403, detail="Permission denied")

    value = await cache.read(key)
    return {"key": key, "value": value, "found": value is not None}


class CacheWriteRequest(BaseModel):
    key: str
    value: object


@app.post("/cache/write", tags=["Cache (MESI)"])
async def cache_write(
    body: CacheWriteRequest,
    authorization: Optional[str] = Header(default=None)
):
    """Write a value to the distributed MESI cache (invalidates other copies)."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "cache:write", body.key):
        raise HTTPException(status_code=403, detail="Permission denied")

    success = await cache.write(body.key, body.value)
    audit.log("CACHE", payload["sub"], body.key, "write", "SUCCESS" if success else "FAIL")
    return {"key": body.key, "success": success, "state": "M"}


@app.delete("/cache/{key}", tags=["Cache (MESI)"])
async def cache_delete(key: str, authorization: Optional[str] = Header(default=None)):
    """Delete a key from all cache nodes."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "cache:delete", key):
        raise HTTPException(status_code=403, detail="Permission denied")

    result = await cache.delete(key)
    return {"key": key, "deleted": result}


@app.get("/cache/status", tags=["Cache (MESI)"])
async def cache_status():
    """Get cache state including MESI state distribution."""
    return cache.get_status()


@app.get("/cache/lines", tags=["Cache (MESI)"])
async def cache_lines():
    """Get all cache lines with their MESI states."""
    return {"lines": cache.get_cache_lines()}


# Internal MESI protocol endpoints
@app.post("/cache/fetch", tags=["Cache (MESI)"], include_in_schema=False)
async def cache_fetch(body: dict = Body(...)):
    return await cache.handle_fetch(body["key"], body["requester"])


@app.post("/cache/invalidate", tags=["Cache (MESI)"], include_in_schema=False)
async def cache_invalidate(body: dict = Body(...)):
    return await cache.handle_invalidate(body["key"], body["invalidator"])


# ══════════════════════════════════════════════════════════════════════════════
# SECURITY / AUTH ENDPOINTS (Bonus D)
# ══════════════════════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    username: str
    role: str = "reader"


@app.post("/auth/login", tags=["Security (Bonus D)"])
async def login(body: LoginRequest):
    """
    Simplified login - returns JWT token.
    In production, would verify credentials against user store.
    """
    if body.role not in ROLES:
        raise HTTPException(status_code=400, detail=f"Invalid role. Valid: {list(ROLES.keys())}")

    token = AuthToken.create(body.username, body.role)
    audit.log("AUTH", body.username, "system", "login", "SUCCESS", {"role": body.role})
    return {
        "access_token": token,
        "token_type": "bearer",
        "role": body.role,
        "permissions": ROLES[body.role]["permissions"],
    }


@app.get("/auth/demo-tokens", tags=["Security (Bonus D)"])
async def demo_tokens():
    """Get demo tokens for all roles (for testing only)."""
    return create_demo_tokens()


@app.get("/auth/audit-log", tags=["Security (Bonus D)"])
async def get_audit_log(
    limit: int = Query(default=50),
    authorization: Optional[str] = Header(default=None)
):
    """Get tamper-proof audit log (admin only)."""
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "admin:all", "audit"):
        raise HTTPException(status_code=403, detail="Admin role required")

    return {
        "entries": audit.get_log(limit),
        "stats": audit.get_stats(),
        "integrity_verified": audit.verify_integrity(),
    }


@app.get("/auth/verify", tags=["Security (Bonus D)"])
async def verify_token(authorization: Optional[str] = Header(default=None)):
    """Verify a JWT token and return its payload."""
    payload = get_auth_payload(authorization)
    return {"valid": True, "payload": payload}



@app.get("/auth/certificates", tags=["Security (Bonus D)"])
async def certificate_status():
    """Get node certificate info and trusted peers (Certificate Management - Bonus D)."""
    if cert_mgr is None:
        return {"error": "Certificate manager not initialized"}
    return cert_mgr.get_status()


@app.post("/auth/trust-peer", tags=["Security (Bonus D)"])
async def trust_peer_cert(
    body: dict = Body(...),
    authorization: Optional[str] = Header(default=None)
):
    """
    Register a peer node's certificate as trusted (admin only).
    In production: exchange certificates during cluster bootstrap.
    """
    payload = get_auth_payload(authorization)
    if not validator.authorize(payload, "admin:all", "certificates"):
        raise HTTPException(status_code=403, detail="Admin role required")

    if cert_mgr is None:
        return {"error": "Certificate manager not initialized"}

    from src.utils.cert_manager import NodeCertificate
    import time as _t
    peer_cert = NodeCertificate(
        node_id=body.get("node_id", "unknown"),
        fingerprint=body.get("fingerprint", ""),
        issued_at=_t.time(),
        expires_at=_t.time() + 365 * 86400,
        public_key_pem=body.get("public_key_pem", ""),
        cert_pem=body.get("cert_pem", ""),
    )
    cert_mgr.trust_peer_certificate(peer_cert)
    audit.log("CERT", payload["sub"], peer_cert.node_id, "trust_cert", "SUCCESS")
    return {"trusted": True, "node_id": peer_cert.node_id, "fingerprint": peer_cert.fingerprint[:16] + "..."}


# ══════════════════════════════════════════════════════════════════════════════
# DEMO ENDPOINTS (for easy testing & video demo)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/demo/scenario/lock-deadlock", tags=["Demo"])
async def demo_lock_deadlock():
    """Simulate a deadlock scenario between two processes."""
    import asyncio

    results = []

    # Process A acquires lock on resource-1
    r1 = await lock_mgr.acquire("resource-1", LockType.EXCLUSIVE, "process-A", timeout=5)
    results.append({"step": 1, "action": "Process-A acquires resource-1", "result": r1})

    # Process B acquires lock on resource-2
    r2 = await lock_mgr.acquire("resource-2", LockType.EXCLUSIVE, "process-B", timeout=5)
    results.append({"step": 2, "action": "Process-B acquires resource-2", "result": r2})

    # Process A tries to acquire resource-2 (deadlock if B waits for 1)
    r3 = await lock_mgr.acquire("resource-2", LockType.EXCLUSIVE, "process-A", wait=False)
    results.append({"step": 3, "action": "Process-A tries resource-2 (no-wait)", "result": r3})

    # Cleanup
    if r1.get("lock_id"):
        await lock_mgr.release(r1["lock_id"], "process-A")
    if r2.get("lock_id"):
        await lock_mgr.release(r2["lock_id"], "process-B")

    return {"scenario": "lock-deadlock", "steps": results}


@app.get("/demo/scenario/cache-mesi", tags=["Demo"])
async def demo_cache_mesi():
    """Demonstrate MESI cache state transitions."""
    steps = []

    # Write (M state)
    await cache.write("demo-key", {"data": "hello MESI"})
    lines = cache.get_cache_lines()
    steps.append({"action": "Write 'demo-key'", "expected_state": "M", "lines": lines})

    # Read (E state if only one copy)
    val = await cache.read("demo-key")
    lines = cache.get_cache_lines()
    steps.append({"action": "Read 'demo-key'", "value": val, "lines": lines})

    return {"scenario": "mesi-states", "steps": steps, "protocol": "MESI"}


@app.get("/demo/scenario/queue-flow", tags=["Demo"])
async def demo_queue_flow():
    """Demonstrate produce → consume → ack flow."""
    results = {}

    # Produce
    msg_id = await queue.produce("demo-queue", {"event": "order_placed", "amount": 99.99}, "shop-service")
    results["produced"] = {"message_id": msg_id, "queue": "demo-queue"}

    # Consume
    msg = await queue.consume("demo-queue", "fulfillment-service", timeout=2.0)
    results["consumed"] = msg

    # Acknowledge
    if msg:
        acked = await queue.acknowledge("demo-queue", msg["message_id"], "fulfillment-service")
        results["acknowledged"] = acked

    stats = await queue.get_queue_stats("demo-queue")
    results["queue_stats"] = stats

    return {"scenario": "queue-flow", **results}


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=config.NODE_HOST,
        port=config.NODE_PORT,
        reload=False,
        log_level=config.LOG_LEVEL.lower(),
    )


# ══════════════════════════════════════════════════════════════════════════════
# EXTRA DEMO ENDPOINTS (Network Partition + Byzantine Attack)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/demo/scenario/network-partition", tags=["Demo"])
async def demo_network_partition():
    """
    Simulate Raft behavior during network partition.
    Shows stale leader rejection and leader step-down on higher term.
    """
    import time as _time
    results = []

    current = raft.get_status()
    results.append({
        "step": 1,
        "action": "Status sebelum partition",
        "state": {"role": current["role"], "term": current["term"], "leader": current["leader_id"]}
    })

    stale_req = {
        "term": max(0, raft.current_term - 1),
        "leader_id": "fake-stale-leader",
        "prev_log_index": -1, "prev_log_term": 0,
        "entries": [], "leader_commit": -1,
    }
    stale_resp = await raft.handle_append_entries(stale_req)
    results.append({
        "step": 2,
        "action": "Menerima AppendEntries dari stale leader (term lama)",
        "request_term": stale_req["term"],
        "current_term": raft.current_term,
        "response_success": stale_resp["success"],
        "explanation": "success=False → Raft menolak stale leader (safety property terjaga)"
    })

    higher_term = raft.current_term + 1
    heal_req = {
        "term": higher_term,
        "leader_id": "node-new-leader",
        "prev_log_index": -1, "prev_log_term": 0,
        "entries": [], "leader_commit": -1,
    }
    heal_resp = await raft.handle_append_entries(heal_req)
    results.append({
        "step": 3,
        "action": "Partition healed — node baru dengan term lebih tinggi",
        "new_term": higher_term,
        "response_success": heal_resp["success"],
        "explanation": "success=True, node otomatis step-down ke FOLLOWER"
    })

    final = raft.get_status()
    results.append({
        "step": 4,
        "action": "Status akhir setelah partition heal",
        "state": {"role": final["role"], "term": final["term"], "leader": final["leader_id"]}
    })

    return {
        "scenario": "network-partition",
        "description": "Raft menolak stale leader & auto step-down saat term lebih tinggi terdeteksi",
        "steps": results
    }


@app.get("/demo/scenario/byzantine-attack", tags=["Demo"])
async def demo_byzantine_attack():
    """
    Simulate Byzantine node attacks on PBFT.
    Shows digest verification and quorum protection.
    """
    from src.consensus.pbft import PBFTNode, compute_digest
    import time as _time
    results = []

    results.append({
        "step": 1,
        "action": "PBFT cluster configuration",
        "pbft_status": pbft.get_status(),
        "formula": f"n={pbft.n} nodes → max f={pbft.f} Byzantine faults tolerated, quorum={pbft._quorum()}"
    })

    tampered_payload = {"action": "STEAL", "amount": 999999}
    correct_payload = {"action": "TRANSFER", "amount": 100}
    wrong_digest = compute_digest(tampered_payload)

    tamper_resp = await pbft.handle_pre_prepare({
        "view": pbft.view, "sequence": 999,
        "digest": wrong_digest,
        "node_id": "malicious-node",
        "payload": correct_payload,
        "timestamp": _time.time()
    })
    results.append({
        "step": 2,
        "action": "Byzantine node kirim PRE-PREPARE dengan digest palsu",
        "attack": "digest(tampered_payload) ≠ digest(correct_payload)",
        "accepted": tamper_resp.get("accepted"),
        "explanation": "accepted=False → SHA-256 digest mismatch terdeteksi, pesan ditolak"
    })

    evil_node = PBFTNode("evil-node", [], is_byzantine=True)
    evil_resp = await evil_node.handle_pre_prepare({
        "view": 0, "sequence": 1, "digest": "any",
        "node_id": "primary", "payload": {"cmd": "ok"}, "timestamp": 0
    })
    results.append({
        "step": 3,
        "action": "Simulasi Byzantine node (is_byzantine=True)",
        "evil_response": evil_resp,
        "explanation": "Byzantine node selalu mengirim accepted=False → tidak cukup untuk corrupt quorum"
    })

    results.append({
        "step": 4,
        "action": "Kesimpulan perlindungan PBFT",
        "protections": {
            "digest_check": "SHA-256 memverifikasi integritas setiap message",
            "quorum": f"Butuh {pbft._quorum()}/{pbft.n} setuju → {pbft.f} Byzantine node tidak bisa corrupt consensus",
            "result": "PBFT aman selama jumlah Byzantine nodes ≤ f"
        }
    })

    return {"scenario": "byzantine-attack", "steps": results}
