"""
Base Node Security Layer (Bonus D: Security & Encryption)
Implementasi:
- JWT Authentication untuk inter-node dan client communication
- RBAC (Role-Based Access Control) 
- Audit Logging (append-only)
- Request signature verification
"""
import time
import hashlib
import hmac
import json
import logging
from typing import Optional
from datetime import datetime, timezone
import jwt
from src.utils.config import config

logger = logging.getLogger(__name__)


# ── RBAC Definitions ──────────────────────────────────────────────────────────

ROLES = {
    "admin": {
        "permissions": ["lock:acquire", "lock:release", "lock:read",
                        "queue:produce", "queue:consume", "queue:read",
                        "cache:read", "cache:write", "cache:delete",
                        "raft:read", "pbft:request", "admin:all"],
    },
    "producer": {
        "permissions": ["queue:produce", "queue:read", "cache:read"],
    },
    "consumer": {
        "permissions": ["queue:consume", "queue:read", "lock:acquire", "lock:release", "cache:read"],
    },
    "reader": {
        "permissions": ["lock:read", "queue:read", "cache:read", "raft:read"],
    },
    "node": {
        # Internal node-to-node communication role
        "permissions": ["lock:acquire", "lock:release", "lock:read",
                        "queue:produce", "queue:consume", "queue:read",
                        "cache:read", "cache:write",
                        "raft:read", "pbft:request"],
    },
}


class AuthToken:
    """JWT-based authentication token for users and nodes."""

    @staticmethod
    def create(subject: str, role: str, extra: dict = None) -> str:
        """Create a signed JWT token."""
        now = int(time.time())
        payload = {
            "sub": subject,
            "role": role,
            "permissions": ROLES.get(role, {}).get("permissions", []),
            "iat": now,
            "exp": now + config.JWT_EXPIRY_HOURS * 3600,
            **(extra or {}),
        }
        return jwt.encode(payload, config.SECRET_KEY, algorithm=config.JWT_ALGORITHM)

    @staticmethod
    def verify(token: str) -> Optional[dict]:
        """Verify and decode a JWT token."""
        try:
            payload = jwt.decode(
                token, config.SECRET_KEY, algorithms=[config.JWT_ALGORITHM]
            )
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("[Auth] Token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"[Auth] Invalid token: {e}")
            return None

    @staticmethod
    def has_permission(payload: dict, permission: str) -> bool:
        """Check if a token payload has the required permission."""
        return permission in payload.get("permissions", [])


class AuditLogger:
    """
    Append-only audit log for security events.
    Each entry is HMAC-signed to detect tampering.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._log: list[dict] = []
        self._prev_hash = "genesis"

    def _compute_hash(self, entry: dict) -> str:
        """Compute hash of log entry chained with previous hash (from entry itself)."""
        prev = entry.get("prev_hash", "genesis")
        content = json.dumps(entry, sort_keys=True) + prev
        return hashlib.sha256(content.encode()).hexdigest()

    def _sign(self, content: str) -> str:
        """HMAC signature for tamper detection."""
        return hmac.new(
            config.SECRET_KEY.encode(), content.encode(), hashlib.sha256
        ).hexdigest()

    def log(self, event_type: str, subject: str, resource: str, action: str,
            result: str, metadata: dict = None):
        """Append an audit log entry."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node_id": self.node_id,
            "event_type": event_type,
            "subject": subject,
            "resource": resource,
            "action": action,
            "result": result,
            "metadata": metadata or {},
            "prev_hash": self._prev_hash,
        }
        entry_hash = self._compute_hash(entry)
        entry["hash"] = entry_hash
        entry["signature"] = self._sign(entry_hash)
        self._prev_hash = entry_hash
        self._log.append(entry)
        logger.debug(f"[Audit] {event_type}: {subject} {action} {resource} → {result}")

    def verify_integrity(self) -> bool:
        """Verify the audit log has not been tampered with."""
        prev = "genesis"
        for entry in self._log:
            check_entry = {k: v for k, v in entry.items() if k not in ("hash", "signature")}
            check_entry["prev_hash"] = prev
            expected_hash = self._compute_hash(check_entry)
            if expected_hash != entry["hash"]:
                logger.error(f"[Audit] TAMPER DETECTED at entry: {entry['timestamp']}")
                return False
            prev = entry["hash"]
        return True

    def get_log(self, limit: int = 100) -> list[dict]:
        return self._log[-limit:]

    def get_stats(self) -> dict:
        event_counts = {}
        for entry in self._log:
            et = entry["event_type"]
            event_counts[et] = event_counts.get(et, 0) + 1
        return {
            "total_entries": len(self._log),
            "event_counts": event_counts,
            "integrity_ok": self.verify_integrity(),
        }


class RequestValidator:
    """Validates incoming HTTP requests for authentication and authorization."""

    def __init__(self, audit: AuditLogger):
        self.audit = audit

    def extract_token(self, authorization_header: Optional[str]) -> Optional[str]:
        if not authorization_header:
            return None
        parts = authorization_header.split(" ")
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1]
        return None

    def authenticate(self, authorization_header: Optional[str]) -> Optional[dict]:
        """Extract and verify JWT from Authorization header."""
        token = self.extract_token(authorization_header)
        if not token:
            return None
        return AuthToken.verify(token)

    def authorize(self, payload: dict, permission: str, resource: str = "*") -> bool:
        """Check if authenticated user/node has required permission."""
        subject = payload.get("sub", "unknown")
        has_perm = AuthToken.has_permission(payload, permission)

        self.audit.log(
            event_type="AUTHORIZATION",
            subject=subject,
            resource=resource,
            action=permission,
            result="GRANTED" if has_perm else "DENIED",
        )
        return has_perm


# ── FastAPI Dependency Helpers ────────────────────────────────────────────────

def create_demo_tokens() -> dict:
    """Create demo tokens for testing all roles."""
    return {
        "admin": AuthToken.create("admin_user", "admin"),
        "producer": AuthToken.create("producer_service", "producer"),
        "consumer": AuthToken.create("consumer_service", "consumer"),
        "reader": AuthToken.create("reader_service", "reader"),
        "node": AuthToken.create(config.NODE_ID, "node", {"node_id": config.NODE_ID}),
    }
