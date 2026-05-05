"""
Centralized configuration management using environment variables.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Node Identity
    NODE_ID: str = os.getenv("NODE_ID", "node1")
    NODE_HOST: str = os.getenv("NODE_HOST", "0.0.0.0")
    NODE_PORT: int = int(os.getenv("NODE_PORT", "8001"))

    # Cluster
    PEERS: list[str] = [
        p.strip() for p in os.getenv("PEERS", "").split(",") if p.strip()
    ]

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")

    # Raft Consensus
    RAFT_ELECTION_TIMEOUT_MIN: int = int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "150"))
    RAFT_ELECTION_TIMEOUT_MAX: int = int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "300"))
    RAFT_HEARTBEAT_INTERVAL: int = int(os.getenv("RAFT_HEARTBEAT_INTERVAL", "50"))

    # Cache
    CACHE_MAX_SIZE: int = int(os.getenv("CACHE_MAX_SIZE", "1000"))
    CACHE_POLICY: str = os.getenv("CACHE_POLICY", "LRU")

    # Security (Bonus D)
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-do-not-use-in-prod")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    JWT_EXPIRY_HOURS: int = int(os.getenv("JWT_EXPIRY_HOURS", "24"))
    ENABLE_TLS: bool = os.getenv("ENABLE_TLS", "false").lower() == "true"

    # Metrics
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9090"))
    ENABLE_METRICS: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @property
    def node_url(self) -> str:
        return f"http://{self.NODE_HOST}:{self.NODE_PORT}"

    @property
    def redis_url(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


config = Config()
