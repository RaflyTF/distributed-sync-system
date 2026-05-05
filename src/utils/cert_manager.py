"""
Certificate Management untuk Node Authentication (Bonus D: Security).
Fitur:
- Self-signed certificate generation per node
- Certificate storage dan loading
- Node identity verification via certificate
- Certificate expiry checking
"""
import os
import json
import time
import hashlib
import logging
from dataclasses import dataclass, asdict
from typing import Optional
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import datetime

logger = logging.getLogger(__name__)

CERT_DIR = os.path.join(os.path.dirname(__file__), "../../certs")


@dataclass
class NodeCertificate:
    """Represents a node's certificate metadata."""
    node_id: str
    fingerprint: str
    issued_at: float
    expires_at: float
    public_key_pem: str
    cert_pem: str

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    @property
    def days_remaining(self) -> int:
        return max(0, int((self.expires_at - time.time()) / 86400))

    def to_dict(self) -> dict:
        d = asdict(self)
        d["is_expired"] = self.is_expired
        d["days_remaining"] = self.days_remaining
        return d


class CertificateManager:
    """
    Manages TLS certificates for inter-node authentication.

    In production, nodes verify each other's certificates before
    accepting RPC calls. This prevents impersonation attacks.

    Simplified for educational purposes:
    - Uses self-signed certificates (no CA)
    - Stores certs in local filesystem
    - Provides fingerprint-based verification
    """

    def __init__(self, node_id: str, cert_dir: str = None):
        self.node_id = node_id
        self.cert_dir = cert_dir or CERT_DIR
        os.makedirs(self.cert_dir, exist_ok=True)
        self._trusted_certs: dict[str, NodeCertificate] = {}
        self._my_cert: Optional[NodeCertificate] = None

    # ── Certificate Generation ────────────────────────────────────────────────

    def generate_node_certificate(self, valid_days: int = 365) -> NodeCertificate:
        """
        Generate a self-signed RSA certificate for this node.
        Used for node identity and optionally TLS.
        """
        # Generate RSA key pair
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # Build certificate subject
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, self.node_id),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Distributed Sync System"),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Node Cluster"),
        ])

        now = datetime.datetime.utcnow()
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=valid_days))
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName(self.node_id),
                    x509.DNSName("localhost"),
                ]),
                critical=False,
            )
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True,
            )
            .sign(private_key, hashes.SHA256(), default_backend())
        )

        # Serialize
        cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode()
        private_key_pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption()
        ).decode()

        # Compute fingerprint (SHA-256 of cert DER)
        fingerprint = hashlib.sha256(
            cert.public_bytes(serialization.Encoding.DER)
        ).hexdigest()

        node_cert = NodeCertificate(
            node_id=self.node_id,
            fingerprint=fingerprint,
            issued_at=time.time(),
            expires_at=time.time() + valid_days * 86400,
            public_key_pem=public_key_pem,
            cert_pem=cert_pem,
        )

        # Save to filesystem
        self._save_certificate(node_cert, private_key_pem)
        self._my_cert = node_cert
        logger.info(f"[CertMgr:{self.node_id}] Generated certificate (fingerprint={fingerprint[:16]}...)")
        return node_cert

    def _save_certificate(self, cert: NodeCertificate, private_key_pem: str):
        """Save certificate and private key to disk."""
        cert_path = os.path.join(self.cert_dir, f"{self.node_id}.cert.pem")
        key_path = os.path.join(self.cert_dir, f"{self.node_id}.key.pem")
        meta_path = os.path.join(self.cert_dir, f"{self.node_id}.meta.json")

        with open(cert_path, "w") as f:
            f.write(cert.cert_pem)
        with open(key_path, "w") as f:
            f.write(private_key_pem)
        with open(meta_path, "w") as f:
            meta = {k: v for k, v in asdict(cert).items() if k != "cert_pem"}
            json.dump(meta, f, indent=2)

        # Secure the private key file
        os.chmod(key_path, 0o600)
        logger.debug(f"[CertMgr:{self.node_id}] Certificate saved to {cert_path}")

    # ── Certificate Loading ────────────────────────────────────────────────────

    def load_or_generate(self) -> NodeCertificate:
        """Load existing certificate or generate a new one."""
        cert_path = os.path.join(self.cert_dir, f"{self.node_id}.cert.pem")
        meta_path = os.path.join(self.cert_dir, f"{self.node_id}.meta.json")

        if os.path.exists(cert_path) and os.path.exists(meta_path):
            try:
                with open(cert_path) as f:
                    cert_pem = f.read()
                with open(meta_path) as f:
                    meta = json.load(f)

                cert = NodeCertificate(
                    node_id=meta["node_id"],
                    fingerprint=meta["fingerprint"],
                    issued_at=meta["issued_at"],
                    expires_at=meta["expires_at"],
                    public_key_pem=meta["public_key_pem"],
                    cert_pem=cert_pem,
                )

                if cert.is_expired:
                    logger.warning(f"[CertMgr:{self.node_id}] Certificate expired, regenerating")
                    return self.generate_node_certificate()

                self._my_cert = cert
                logger.info(f"[CertMgr:{self.node_id}] Loaded existing certificate (expires in {cert.days_remaining} days)")
                return cert
            except Exception as e:
                logger.error(f"[CertMgr:{self.node_id}] Failed to load cert: {e}, regenerating")

        return self.generate_node_certificate()

    # ── Certificate Trust ─────────────────────────────────────────────────────

    def trust_peer_certificate(self, peer_cert: NodeCertificate):
        """Add a peer's certificate to the trusted store."""
        self._trusted_certs[peer_cert.node_id] = peer_cert
        logger.info(f"[CertMgr:{self.node_id}] Trusted cert for {peer_cert.node_id} (fp={peer_cert.fingerprint[:16]}...)")

    def verify_peer(self, node_id: str, fingerprint: str) -> bool:
        """
        Verify a peer's identity using certificate fingerprint.
        Returns True if the fingerprint matches a trusted certificate.
        """
        trusted = self._trusted_certs.get(node_id)
        if not trusted:
            logger.warning(f"[CertMgr:{self.node_id}] Unknown peer: {node_id}")
            return False
        if trusted.is_expired:
            logger.warning(f"[CertMgr:{self.node_id}] Peer {node_id} certificate expired")
            return False
        if trusted.fingerprint != fingerprint:
            logger.error(f"[CertMgr:{self.node_id}] Fingerprint MISMATCH for {node_id}! Possible MITM attack!")
            return False
        return True

    # ── Status ────────────────────────────────────────────────────────────────

    def get_my_certificate_info(self) -> Optional[dict]:
        if not self._my_cert:
            return None
        return self._my_cert.to_dict()

    def get_trusted_peers(self) -> list[dict]:
        return [
            {
                "node_id": cert.node_id,
                "fingerprint": cert.fingerprint[:16] + "...",
                "expires_in_days": cert.days_remaining,
                "is_expired": cert.is_expired,
            }
            for cert in self._trusted_certs.values()
        ]

    def get_status(self) -> dict:
        return {
            "node_id": self.node_id,
            "has_certificate": self._my_cert is not None,
            "my_cert": self.get_my_certificate_info(),
            "trusted_peers": self.get_trusted_peers(),
            "cert_dir": self.cert_dir,
        }
