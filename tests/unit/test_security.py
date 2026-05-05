"""Unit tests for Security Layer: JWT Auth, RBAC, and Audit Logging (Bonus D)."""
import time
import pytest
import jwt
from src.nodes.base_node import AuthToken, AuditLogger, RequestValidator, ROLES, create_demo_tokens
from src.utils.config import config


class TestAuthToken:
    def test_create_and_verify(self):
        token = AuthToken.create("user1", "admin")
        payload = AuthToken.verify(token)
        assert payload is not None
        assert payload["sub"] == "user1"
        assert payload["role"] == "admin"

    def test_verify_invalid_token(self):
        result = AuthToken.verify("not.a.valid.token")
        assert result is None

    def test_verify_expired_token(self):
        # Create an already-expired token
        payload = {
            "sub": "user",
            "role": "reader",
            "permissions": [],
            "iat": int(time.time()) - 7200,
            "exp": int(time.time()) - 3600,  # Expired 1 hour ago
        }
        token = jwt.encode(payload, config.SECRET_KEY, algorithm=config.JWT_ALGORITHM)
        assert AuthToken.verify(token) is None

    def test_permissions_included_in_token(self):
        for role, role_data in ROLES.items():
            token = AuthToken.create(f"user_{role}", role)
            payload = AuthToken.verify(token)
            assert payload["permissions"] == role_data["permissions"]

    def test_has_permission_true(self):
        token = AuthToken.create("admin", "admin")
        payload = AuthToken.verify(token)
        assert AuthToken.has_permission(payload, "admin:all") is True

    def test_has_permission_false(self):
        token = AuthToken.create("reader", "reader")
        payload = AuthToken.verify(token)
        assert AuthToken.has_permission(payload, "cache:write") is False

    def test_node_role_permissions(self):
        """Node role should have inter-node communication permissions."""
        token = AuthToken.create("node2", "node")
        payload = AuthToken.verify(token)
        assert AuthToken.has_permission(payload, "lock:acquire") is True
        assert AuthToken.has_permission(payload, "cache:read") is True

    def test_producer_cannot_consume(self):
        token = AuthToken.create("producer1", "producer")
        payload = AuthToken.verify(token)
        assert AuthToken.has_permission(payload, "queue:produce") is True
        assert AuthToken.has_permission(payload, "queue:consume") is False

    def test_consumer_cannot_write_cache(self):
        token = AuthToken.create("consumer1", "consumer")
        payload = AuthToken.verify(token)
        assert AuthToken.has_permission(payload, "queue:consume") is True
        assert AuthToken.has_permission(payload, "cache:write") is False


class TestAuditLogger:
    @pytest.fixture
    def audit(self):
        return AuditLogger("node1")

    def test_empty_log_has_integrity(self, audit):
        assert audit.verify_integrity() is True

    def test_log_creates_entry(self, audit):
        audit.log("LOCK", "user1", "resource-A", "acquire", "SUCCESS")
        entries = audit.get_log()
        assert len(entries) == 1
        assert entries[0]["event_type"] == "LOCK"
        assert entries[0]["subject"] == "user1"
        assert entries[0]["result"] == "SUCCESS"

    def test_multiple_entries_maintain_integrity(self, audit):
        for i in range(10):
            audit.log("TEST", f"user{i}", f"resource-{i}", "action", "SUCCESS")
        assert audit.verify_integrity() is True
        assert len(audit.get_log()) == 10

    def test_entries_are_chained(self, audit):
        """Each entry should reference the hash of the previous entry."""
        audit.log("E1", "u1", "r1", "a1", "OK")
        audit.log("E2", "u2", "r2", "a2", "OK")
        entries = audit.get_log()
        assert entries[1]["prev_hash"] == entries[0]["hash"]

    def test_tamper_detection(self, audit):
        """Modifying an entry should break integrity."""
        audit.log("SENSITIVE", "admin", "system", "delete", "SUCCESS")
        # Tamper with the entry
        audit._log[0]["result"] = "FAILURE"
        assert audit.verify_integrity() is False

    def test_get_log_limit(self, audit):
        for i in range(20):
            audit.log("TEST", "u", "r", "a", "OK")
        assert len(audit.get_log(limit=5)) == 5
        assert len(audit.get_log(limit=100)) == 20

    def test_audit_stats(self, audit):
        audit.log("LOCK", "u1", "r1", "acquire", "SUCCESS")
        audit.log("LOCK", "u2", "r2", "acquire", "SUCCESS")
        audit.log("AUTH", "u3", "system", "login", "SUCCESS")
        stats = audit.get_stats()
        assert stats["total_entries"] == 3
        assert stats["event_counts"]["LOCK"] == 2
        assert stats["event_counts"]["AUTH"] == 1

    def test_entries_have_required_fields(self, audit):
        audit.log("TEST", "subject", "resource", "action", "result", {"extra": "data"})
        entry = audit.get_log()[0]
        for field in ["timestamp", "node_id", "event_type", "subject",
                      "resource", "action", "result", "hash", "signature", "prev_hash"]:
            assert field in entry, f"Missing field: {field}"


class TestRequestValidator:
    @pytest.fixture
    def validator(self):
        audit = AuditLogger("node1")
        return RequestValidator(audit)

    def test_extract_token_valid_bearer(self, validator):
        token = AuthToken.create("user1", "admin")
        extracted = validator.extract_token(f"Bearer {token}")
        assert extracted == token

    def test_extract_token_missing_header(self, validator):
        assert validator.extract_token(None) is None
        assert validator.extract_token("") is None

    def test_extract_token_wrong_scheme(self, validator):
        assert validator.extract_token("Basic abc123") is None

    def test_authenticate_valid(self, validator):
        token = AuthToken.create("user1", "admin")
        payload = validator.authenticate(f"Bearer {token}")
        assert payload is not None
        assert payload["sub"] == "user1"

    def test_authenticate_invalid(self, validator):
        payload = validator.authenticate("Bearer invalid.token.here")
        assert payload is None

    def test_authorize_correct_permission(self, validator):
        token = AuthToken.create("admin", "admin")
        payload = AuthToken.verify(token)
        assert validator.authorize(payload, "admin:all", "system") is True

    def test_authorize_missing_permission(self, validator):
        token = AuthToken.create("reader", "reader")
        payload = AuthToken.verify(token)
        assert validator.authorize(payload, "cache:write", "key1") is False

    def test_authorize_creates_audit_entry(self, validator):
        token = AuthToken.create("user1", "admin")
        payload = AuthToken.verify(token)
        validator.authorize(payload, "lock:acquire", "resource-1")
        assert len(validator.audit.get_log()) >= 1


class TestDemoTokens:
    def test_demo_tokens_all_roles(self):
        tokens = create_demo_tokens()
        for role in ["admin", "producer", "consumer", "reader", "node"]:
            assert role in tokens
            payload = AuthToken.verify(tokens[role])
            assert payload is not None
            assert payload["role"] == role
